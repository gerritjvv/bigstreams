package org.streams.collector.server.impl;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.streams.collector.mon.CollectorStatus;
import org.streams.collector.write.LogFileWriter;
import org.streams.commons.compression.CompressionPool;
import org.streams.commons.compression.CompressionPoolFactory;
import org.streams.commons.file.CoordinationException;
import org.streams.commons.file.CoordinationServiceClient;
import org.streams.commons.file.FileStatus;
import org.streams.commons.file.PostWriteAction;
import org.streams.commons.file.SyncPointer;
import org.streams.commons.io.Header;
import org.streams.commons.io.NetworkCodes;
import org.streams.commons.io.Protocol;
import org.streams.commons.metrics.CounterMetric;
import org.streams.commons.util.concurrent.KeyLock;

/**
 * 
 * This class implements the writing of the agent logs to the collect local
 * disk.
 * 
 */
public class LogWriterHandler extends SimpleChannelUpstreamHandler {

	private static final Logger LOG = Logger.getLogger(LogWriterHandler.class);

	Protocol protocol;
	Configuration configuration;
	org.apache.hadoop.conf.Configuration hadoopConf;
	LogFileWriter writer;
	CoordinationServiceClient coordinationService;

	CollectorStatus collectorStatus;

	CounterMetric fileBytesWrittenMetric;

	CompressionPoolFactory compressionPoolFactory;

	/**
	 * This lock will check for the collector that an agent is sending only one
	 * send request for a certain file. This still means agents can send
	 * multiple send requests for multiple files.
	 */
	static final KeyLock agentFileLocalLock = new KeyLock(500);

	/**
	 * Time that this class will wait for a compression resource to become
	 * available. Default 10000L
	 */
	private long waitForCompressionResource = 10000L;

	/**
	 * Each request will contain a Header with the compression codec to use for
	 * reading the message.<br/>
	 * A Map is used to not have to re-instantiate and configure the same
	 * compression codecs with each request.<br/>
	 */
	private static Map<String, CompressionCodec> codecMap = new ConcurrentHashMap<String, CompressionCodec>();

	/**
	 * Simple concurrency check. This is experimental and would either be
	 * improved appon or removed in the future depending on performance impact.
	 */
	// private static final ConcurrentMap<FileTrackingStatus, Long>
	// fileStatusMap = new ConcurrentHashMap<FileTrackingStatus, Long>();

	public LogWriterHandler() {
	}

	public LogWriterHandler(Protocol protocol, Configuration configuration,
			org.apache.hadoop.conf.Configuration hadoopConf,
			LogFileWriter writer,
			CoordinationServiceClient coordinationService,
			CollectorStatus collectorStatus,
			CounterMetric fileBytesWrittenMetric,
			CompressionPoolFactory compressionPoolFactory) {
		super();
		this.protocol = protocol;
		this.configuration = configuration;
		this.hadoopConf = hadoopConf;
		this.writer = writer;
		this.coordinationService = coordinationService;
		this.collectorStatus = collectorStatus;
		this.fileBytesWrittenMetric = fileBytesWrittenMetric;
		this.compressionPoolFactory = compressionPoolFactory;
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {

		AgentSession session = (AgentSession) ctx.getAttachment();
		if (session != null) {
			if (!session.messageReceived) {
				LOG.warn("The agent " + session.getRemoteAddress()
						+ " connected but did not send data");
			}
		}

		collectorStatus.decCounter(
				CollectorStatus.COUNTERS.CHANNELS_OPEN.toString(), 1);
		super.channelClosed(ctx, e);
	}

	/**
	 * On Message: (1) get channelBuffer (2) create Data Input Stream (3) Read
	 * Header data (4) Get Codec (5) Build FileTrackingStatus (6) Aquire Local
	 * Lock (7) withLock (7).1 If Sync (7).1.a Write to file (7).1.b write 200
	 * to buffer (7).2 If Sync Conflict (7).2.a Write Conflict to buffer
	 * (8)write to Agent (9)Close Input Streams (10)Release Local Lock
	 */
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {

		final ChannelBuffer buff = (ChannelBuffer) e.getMessage();

		final ChannelBufferInputStream channelInput = new ChannelBufferInputStream(
				buff);
		final DataInputStream datInput = new DataInputStream(channelInput);

		if (!buff.readable()) {
			throw new RuntimeException("The channel buffer is not readable");
		}
		// read header
		final Header header = protocol.read(configuration, datInput);
		// instantiate the CompressionCodec sent with the Header
		final CompressionCodec codec = getCodec(header);

		final String agentName = header.getHost();
		final String fileName = header.getFileName();
		final String logType = header.getLogType();

		final FileStatus.FileTrackingStatus fileStatus = buildFileStatus(header);

		final AgentSession agentSession = (AgentSession) ctx.getAttachment();
		agentSession.setMessageReceived();
		agentSession.setAgentName(agentName);
		agentSession.setFileName(fileName);
		agentSession.setLogType(logType);

		final String localKey = agentName + logType + fileName;
		boolean localLockAcquired = false;

		try {

			localLockAcquired = LogWriterHandler.agentFileLocalLock
					.acquireLock(localKey, 2000L);

			if (!localLockAcquired) {
				// if no local lock could be acquired within 2 seconds
				// we throw
				// and coordination exception
				throw new CoordinationException(
						"Local lock in collector could not be obtained (tried for 2 seconds) for agent: "
								+ fileStatus.getAgentName()
								+ " log type: "
								+ fileStatus.getLogType()
								+ " file name: "
								+ fileStatus.getFileName());
			}

			// SYNC GLOBALLY
			final ChannelBuffer buffer = coordinationService
					.withLock(
							fileStatus,
							new CoordinationServiceClient.CoordinationServiceListener<ChannelBuffer>() {
								@Override
								public ChannelBuffer syncConflict(
										FileStatus.FileTrackingStatus file,
										SyncPointer pointer) throws Exception {

									long syncFilePointer = pointer
											.getFilePointer();
									LOG.info("File pointer Conflict detected: agent "
											+ header.getHost()
											+ " file: "
											+ header.getFileName()
											+ " agentPointer: "
											+ file.getFilePointer()
											+ " headerPointer: "
											+ header.getFilePointer()
											+ " collectorPointer: "
											+ syncFilePointer
											+ " syncid: "
											+ pointer.getTimeStamp());
									
									ChannelBuffer buffer = ChannelBuffers
											.dynamicBuffer();
									buffer.writeInt(NetworkCodes.CODE.SYNC_CONFLICT
											.num());
									buffer.writeLong(syncFilePointer);
									return buffer;
								}

								@Override
								public ChannelBuffer inSync(
										FileStatus.FileTrackingStatus file,
										final SyncPointer pointer,
										PostWriteAction writeAction)
										throws Exception {

									// write the data from agent to file
									writeToFile(codec, datInput, agentSession,
											pointer, file, writeAction);

									ChannelBuffer buffer = ChannelBuffers
											.dynamicBuffer();
									buffer.writeInt(200);
									return buffer;
								}

							});

			// write responses back to agent
			writeToAgent(e, buffer, agentSession);

		} finally {

			IOUtils.closeQuietly(datInput);
			IOUtils.closeQuietly(channelInput);

			// on any error event with coordination these resources must
			// be
			// released
			// assert fileStatusMap.remove(fileStatus) != null;
			if (localLockAcquired) {
				agentFileLocalLock.releaseLock(localKey);
			}

		}

		// log the agent session
		if (LOG.isDebugEnabled()) {
			LOG.debug(agentSession.toString());
		}
	}

	/**
	 * Builder a FileStatus.FileTrackingStatus instance from the header data.
	 * 
	 * @param header
	 * @return
	 */
	private static final FileStatus.FileTrackingStatus buildFileStatus(
			Header header) {

		org.streams.commons.file.FileStatus.FileTrackingStatus.Builder builder = FileStatus.FileTrackingStatus
				.newBuilder();

		builder.setDate(System.currentTimeMillis());
		builder.setFilePointer(header.getFilePointer());
		builder.setLinePointer(header.getLinePointer());

		builder.setFileSize(header.getFileSize());
		builder.setAgentName(header.getHost());
		builder.setFileName(header.getFileName());
		builder.setLogType(header.getLogType());
		Date fileDate = header.getFileDate();
		if(fileDate == null){
			builder.setFileDate(System.currentTimeMillis());
		}else{
			builder.setFileDate(fileDate.getTime());
		}

		return builder.build();
	}

	/**
	 * Write response back to agent
	 * 
	 * @param e
	 * @param buffer
	 * @param agentSession
	 */
	private final void writeToAgent(MessageEvent e, ChannelBuffer buffer,
			AgentSession agentSession) {

		// write responses back to agent
		ChannelFuture future = e.getChannel().write(buffer);
		agentSession.setSentResponseRequest();

		agentSession.setReleasedCoordinationLock();
		// check slow writes:
		if ((agentSession.getFileWriteEndTime() - agentSession
				.getFileWriteStartTime()) > 500) {
			LOG.error("File writing is slowing down please check the log directory");
		}

		if (future != null) {
			future.addListener(new ChannelFutureListener() {

				@Override
				public void operationComplete(ChannelFuture f) throws Exception {
					try {
						ChannelFutureListener.CLOSE.operationComplete(f);
					} catch (Throwable t) {
						LOG.error("ERROR While closing channel :"
								+ f.getChannel() + " " + f.getCause());
					}
				}

			});

		}
	}

	/**
	 * Write data to file
	 * 
	 * @param codec
	 * @param datInput
	 * @param agentSession
	 * @param pointer
	 * @param fileStatus
	 * @param writeAction
	 * @throws Exception
	 */
	private final void writeToFile(CompressionCodec codec,
			InputStream datInput, AgentSession agentSession,
			final SyncPointer pointer,
			final FileStatus.FileTrackingStatus fileStatus,
			PostWriteAction writeAction) throws Exception {

		CompressionPool pool = compressionPoolFactory.get(codec);

		CompressionInputStream compressInput = pool.create(datInput,
				waitForCompressionResource, TimeUnit.MILLISECONDS);

		if (compressInput == null) {
			throw new IOException("No compression resource available for "
					+ codec);
		}

		long bytesWritten = -1;
		agentSession.setAcquiredCoordinationLock();
		if (LOG.isDebugEnabled()) {
			LOG.debug("LOCK(" + pointer.getLockId() + ")");
		}

		agentSession.setFileWriteStartTime();

		try {
			// register to session that the
			// coordination lock could be acquired

			bytesWritten = writer.write(fileStatus, compressInput, writeAction);
		} finally {
			pool.closeAndRelease(compressInput);
		}

		// register that the file was written
		agentSession.setWrittenToFile();

		if (bytesWritten > -1) {
			// send kilobytes written
			fileBytesWrittenMetric.incrementCounter(bytesWritten / 1024);
		}
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

		// although we might create more than one CompressionCodec
		// we don't care based its more expensive to synchronise than simply
		// creating more than one instance of the CompressionCodec
		CompressionCodec codec = codecMap.get(codecClassName);

		if (codec == null) {
			codec = (CompressionCodec) Thread.currentThread()
					.getContextClassLoader().loadClass(codecClassName)
					.newInstance();

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

		AgentSession agentSession = (AgentSession) ctx.getAttachment();

		if (agentSession == null) {
			agentSession = new AgentSession("unkown");
		}

		Throwable exception = e.getCause();
		NetworkCodes.CODE code = null;
		CollectorStatus.STATUS stat = null;

		if (exception instanceof java.net.ConnectException) {
			code = NetworkCodes.CODE.COORDINATION_CONNECTION_ERROR;
			stat = CollectorStatus.STATUS.COORDINATION_ERROR;
		} else if (exception instanceof CoordinationException) {
			CoordinationException coordExcp = (CoordinationException) exception;
			if (coordExcp.isConnectException()) {
				code = NetworkCodes.CODE.COORDINATION_CONNECTION_ERROR;
				stat = CollectorStatus.STATUS.COORDINATION_ERROR;
			} else {
				code = NetworkCodes.CODE.COORDINATION_LOCK_ERROR;
				stat = CollectorStatus.STATUS.COORDINATION_LOCK_ERROR;
			}
		} else {
			code = NetworkCodes.CODE.UNKOWN;
			stat = CollectorStatus.STATUS.UNKOWN_ERROR;
		}

		// WRITE STATUS
		try {

			/**
			 * Very important to respond here. The agent will always be
			 * listening for some kind of feedback.
			 */
			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
			buffer.writeInt(code.num());
			buffer.writeBytes(code.msg().getBytes("UTF-8"));

			if (e.getChannel().isOpen()) {

				ChannelFuture future = e.getChannel().write(buffer);

				future.addListener(ChannelFutureListener.CLOSE);

			} else {
				LOG.error("Channel was closed by agent "
						+ agentSession.getAgentName() + ": exception: "
						+ code.num() + " " + code.msg() + " cause: "
						+ exception + " cannot be written to agent");
			}
			collectorStatus.setStatus(stat, e.getCause().toString());

			collectorStatus.incCounter("Errors_Caught", 1);

			LOG.error(agentSession.toString());
			LOG.error(exception.toString(), exception);
			
			// LOG.error(exception, exception);
		} catch (Throwable t) {
			LOG.error("Throwed exception in catchException " + t);
		}

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

	public CompressionPoolFactory getCompressionPoolFactory() {
		return compressionPoolFactory;
	}

	public void setCompressionPoolFactory(
			CompressionPoolFactory compressionPoolFactory) {
		this.compressionPoolFactory = compressionPoolFactory;
	}

	public long getWaitForCompressionResource() {
		return waitForCompressionResource;
	}

	public void setWaitForCompressionResource(long waitForCompressionResource) {
		this.waitForCompressionResource = waitForCompressionResource;
	}

	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {

		AgentSession session = new AgentSession(e.getChannel()
				.getRemoteAddress().toString());
		ctx.setAttachment(session);

		super.channelOpen(ctx, e);
	}

}
