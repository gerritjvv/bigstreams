package org.streams.collector.server.impl;

import java.io.DataInputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.streams.collector.mon.CollectorStatus;
import org.streams.collector.write.LogFileWriter;
import org.streams.commons.file.CoordinationServiceClient;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.SyncPointer;
import org.streams.commons.io.Header;
import org.streams.commons.io.Protocol;


/**
 * 
 * This class implements the writing of the agent logs to the collect local
 * disk.
 * 
 */
public class LogWriterHandler extends SimpleChannelHandler {

	private static final Logger LOG = Logger.getLogger(LogWriterHandler.class);

	Protocol protocol;
	Configuration configuration;
	org.apache.hadoop.conf.Configuration hadoopConf;
	LogFileWriter writer;
	CoordinationServiceClient coordinationService;

	CollectorStatus collectorStatus;

	/**
	 * Each request will contain a Header with the compression codec to use for
	 * reading the message.<br/>
	 * A Map is used to not have to re-instantiate and configure the same
	 * compression codecs with each request.<br/>
	 */
	private Map<String, CompressionCodec> codecMap = new ConcurrentHashMap<String, CompressionCodec>();

	public LogWriterHandler() {
	}

	public LogWriterHandler(Protocol protocol, Configuration configuration,
			org.apache.hadoop.conf.Configuration hadoopConf,
			LogFileWriter writer,
			CoordinationServiceClient coordinationService,
			CollectorStatus collectorStatus) {
		super();
		this.protocol = protocol;
		this.configuration = configuration;
		this.hadoopConf = hadoopConf;
		this.writer = writer;
		this.coordinationService = coordinationService;
		this.collectorStatus = collectorStatus;
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		collectorStatus.decCounter(
				CollectorStatus.COUNTERS.CHANNELS_OPEN.toString(), 1);
		super.channelClosed(ctx, e);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {

		ChannelBuffer buff = (ChannelBuffer) e.getMessage();

		ChannelBufferInputStream channelInput = new ChannelBufferInputStream(
				buff);
		DataInputStream datInput = new DataInputStream(channelInput);

		if (!buff.readable()){
			throw new RuntimeException("The channel buffer is not readable");
		}
		
		// read header
		Header header = protocol.read(configuration, datInput);

		// instantiate the CompressionCodec sent with the Header
		CompressionCodec codec = getCodec(header);
		
		CompressionInputStream compressInput = codec
				.createInputStream(datInput);

		String agentName = header.getHost();
		String fileName = header.getFileName();
		String logType = header.getLogType();

		FileTrackingStatus fileStatus = new FileTrackingStatus(
				header.getFilePointer(), header.getFileSize(),
				header.getLinePointer(), agentName, fileName, logType);

		// --------------------- Check Coordination parameters
		// --------------------- that is that the agent is not sending duplicate
		// data

		// We synchronise here on agent + log type + fileName to prevent any
		// agent sending more than one request per file at a time.
		// during normal expected operation there will only ever be one agent
		// message per agent + fileName
		// but the collector has to guard against this possible event.
		SyncPointer syncPointer = null;

		ChannelFuture future = null;

		try {
			syncPointer = coordinationService.getAndLock(fileStatus);

			if (syncPointer == null) {
				collectorStatus.setStatus(CollectorStatus.STATUS.UNKOWN_ERROR,
						"File already Locked ERROR " + fileName);
				throw new RuntimeException("File already locked " + fileName);
			}

			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();

			final long syncFilePointer = syncPointer.getFilePointer();
			final long filePointer = fileStatus.getFilePointer();

			if (syncFilePointer == filePointer) {
				//
				// @TODO BETTER ERROR HANDLING AND ROLLBACK STRATEGY FOR FILE
				// Its trivial to rollback the db persistence all here but
				// aftear
				// having written to a file and if the db call
				// fails afterwards how to rollback the written file???
				// write data to file
				int bytesWritten = writer.write(fileStatus, compressInput);

				syncPointer.incFilePointer(bytesWritten);

				buffer.writeInt(200);
			} else {
				LOG.info("File pointer Conflict detected: agent "
						+ header.getHost() + " file: " + header.getFileName()
						+ " agentPointer: " + header.getFilePointer()
						+ " collectorPointer: " + syncFilePointer);

				// send the sync pointer to the agent, this is a request made to
				// the
				// agent that is sends data starting from this pointer
				// write the http codec 409 == Conflict
				buffer.writeInt(409);
				buffer.writeLong(syncFilePointer);
			}

			future = e.getChannel().write(buffer);
		} finally {
			if (syncPointer != null) {
				coordinationService.saveAndFreeLock(syncPointer);
			}
			if(compressInput != null){
				IOUtils.closeQuietly(compressInput);
				IOUtils.closeQuietly(datInput);
				IOUtils.closeQuietly(channelInput);
			}
			
		}

		future.addListener(ChannelFutureListener.CLOSE);

		collectorStatus.setStatus(CollectorStatus.STATUS.OK, "Running");

	}

	/**
	 * Returns the CompressionCodec for the message.
	 * 
	 * @param header
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	private final CompressionCodec getCodec(Header header)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {
		String codecClassName = header.getCodecClassName();

		//although we might create more than one CompressionCodec
		//we don't care based its more expensive to synchronise than simply
		//creating more than one instance of the CompressionCodec
		CompressionCodec codec = codecMap.get(codecClassName);

		if (codec == null) {
			codec = (CompressionCodec) Thread.currentThread()
					.getContextClassLoader()
					.loadClass(codecClassName).newInstance();

			if (codec instanceof Configurable) {
				((Configurable) codec).setConf(hadoopConf);
			}
			
			codecMap.put(codecClassName, codec);
		}

		return codec;
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		collectorStatus.incCounter(
				CollectorStatus.COUNTERS.CHANNELS_OPEN.toString(), 1);
		super.channelConnected(ctx, e);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		collectorStatus.setStatus(CollectorStatus.STATUS.UNKOWN_ERROR, e
				.getCause().toString());

		collectorStatus.incCounter("Errors_Caught", 1);

		LOG.error(e.getCause().toString(), e.getCause());
		e.getChannel().close();
	}

	public Protocol getProtocol() {
		return protocol;
	}

	public void setProtocol(Protocol protocol) {
		this.protocol = protocol;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public org.apache.hadoop.conf.Configuration getHadoopConf() {
		return hadoopConf;
	}

	public void setHadoopConf(org.apache.hadoop.conf.Configuration hadoopConf) {
		this.hadoopConf = hadoopConf;
	}

	public CoordinationServiceClient getCoordinationService() {
		return coordinationService;
	}

	public void setCoordinationService(
			CoordinationServiceClient coordinationService) {
		this.coordinationService = coordinationService;
	}

	public CollectorStatus getCollectorStatus() {
		return collectorStatus;
	}

	public void setCollectorStatus(CollectorStatus collectorStatus) {
		this.collectorStatus = collectorStatus;
	}

	public LogFileWriter getWriter() {
		return writer;
	}

	public void setWriter(LogFileWriter writer) {
		this.writer = writer;
	}

}
