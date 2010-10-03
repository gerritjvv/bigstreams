package org.streams.collector.server.impl;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import org.jboss.netty.channel.SimpleChannelHandler;
import org.streams.collector.error.ConcurrencyException;
import org.streams.collector.mon.CollectorStatus;
import org.streams.collector.write.LogFileWriter;
import org.streams.collector.write.PostWriteAction;
import org.streams.commons.compression.CompressionPool;
import org.streams.commons.compression.CompressionPoolFactory;
import org.streams.commons.file.CoordinationException;
import org.streams.commons.file.CoordinationServiceClient;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.SyncPointer;
import org.streams.commons.io.Header;
import org.streams.commons.io.Protocol;
import org.streams.commons.metrics.CounterMetric;

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

	CounterMetric fileBytesWrittenMetric;

	CompressionPoolFactory compressionPoolFactory;

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
	private Map<String, CompressionCodec> codecMap = new ConcurrentHashMap<String, CompressionCodec>();

	/**
	 * Simple concurrency check. This is experimental and would either be
	 * improved appon or removed in the future depending on performance impact.
	 */
	private static final ConcurrentMap<FileTrackingStatus, Long> fileStatusMap = new ConcurrentHashMap<FileTrackingStatus, Long>();

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
		collectorStatus.decCounter(
				CollectorStatus.COUNTERS.CHANNELS_OPEN.toString(), 1);
		super.channelClosed(ctx, e);
	}

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

		final FileTrackingStatus fileStatus = new FileTrackingStatus(
				header.getFilePointer(), header.getFileSize(),
				header.getLinePointer(), agentName, fileName, logType);

		ChannelFuture future = null;
		int bytesWritten = -1;

		CompressionPool pool = compressionPoolFactory.get(codec);

		CompressionInputStream compressInput = pool.create(datInput,
				waitForCompressionResource, TimeUnit.MILLISECONDS);
		boolean compressInputWasReleased = false;

		if (compressInput == null) {
			throw new IOException("No compression resource available for "
					+ codec);
		}

		try {

			// check that the file status is not currently been used
			// we only expect one filestatus at any time, having 2 means that
			// there is a concurrency error
			checkFileStatusConcurrency(fileStatus);

			// --------------------- Check Coordination parameters
			// --------------------- that is that the agent is not sending
			// duplicate
			// data

			// We synchronise here on agent + log type + fileName to prevent any
			// agent sending more than one request per file at a time.
			// during normal expected operation there will only ever be one
			// agent
			// message per agent + fileName
			// but the collector has to guard against this possible event.

			final SyncPointer syncPointer = coordinationService
					.getAndLock(fileStatus);

			if (syncPointer == null) {
				collectorStatus.setStatus(CollectorStatus.STATUS.UNKOWN_ERROR,
						"File already Locked ERROR " + fileName);
				throw new CoordinationException("File already locked "
						+ fileName);
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("LOCK(" + syncPointer.getLockId() + ")");
			}

			final long syncFilePointer = syncPointer.getFilePointer();
			final long filePointer = fileStatus.getFilePointer();

			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();

			if (syncFilePointer == filePointer) {

				try {
					//Note on rollback:
					// The writer will writer the file data, and then execute the PostWriteAction
					// if any step fails the file will be rolled back.
					// This mean that if the syncPointer release send to the CoordinationService fails the file is rolled back
					// and an error thrown.
					bytesWritten = writer.write(fileStatus, compressInput,
							new PostWriteAction() {

								@Override
								public void run(int bytesWritten)
										throws Exception {
									//INCREMENT FILE SYNCPOINTER
									syncPointer.incFilePointer(bytesWritten);
									sendSyncRelease(syncPointer);
									
								}
							});
				} finally {
					pool.closeAndRelease(compressInput);
					compressInputWasReleased = true;
					IOUtils.closeQuietly(datInput);
					IOUtils.closeQuietly(channelInput);
				}


				buffer.writeInt(200);
			} else {
				LOG.info("File pointer Conflict detected: agent "
						+ header.getHost() + " file: " + header.getFileName()
						+ " agentPointer: " + header.getFilePointer()
						+ " collectorPointer: " + syncFilePointer);

				// send the sync pointer to the agent, this is a request
				// made to
				// the
				// agent that is sends data starting from this pointer
				// write the http codec 409 == Conflict
				buffer.writeInt(409);
				buffer.writeLong(syncFilePointer);
			}

			future = e.getChannel().write(buffer);

		} finally {
			// on any error event with coordination these resources must be
			// released
			fileStatusMap.remove(fileStatus);
			if (compressInputWasReleased) {
				pool.closeAndRelease(compressInput);
			}

		}

		if (future != null) {
			future.addListener(ChannelFutureListener.CLOSE);
		}

		collectorStatus.setStatus(CollectorStatus.STATUS.OK, "Running");
		if (bytesWritten > -1) {
			//send kilobytes written
			fileBytesWrittenMetric.incrementCounter(bytesWritten/1024);
		}

	}

	/**
	 * Calls the saveAndFreeLock method on the coordination service.
	 * @param syncPointer
	 */
	private final void sendSyncRelease(SyncPointer syncPointer) {

		try {
			// if we fail to attain a lock we shoud roll the file back
			coordinationService.saveAndFreeLock(syncPointer);
			if (LOG.isDebugEnabled()) {
				LOG.debug("UNLOCK(" + syncPointer.getLockId() + ")");
			}
		} catch (Exception unlockError) {
			LOG.error("Error while unlocking (" + syncPointer.getLockId()
					+ ") - " + unlockError.toString(), unlockError);

			throwException(unlockError);
		}

	}

	/**
	 * Helper method to throw an exception with a correct stack trace.
	 */
	private static final void throwException(Throwable t)
			throws RuntimeException {
		RuntimeException writerException = new RuntimeException(t.toString(), t);
		writerException.setStackTrace(t.getStackTrace());
		throw writerException;
	}

	/**
	 * This method will check to see if another thread is processing the
	 * FileTrackingStatus. If so a ConcurrencyException is thrown.
	 * 
	 * @param fileStatus
	 */
	private void checkFileStatusConcurrency(FileTrackingStatus fileStatus) {
		Long ts = null;

		if ((ts = fileStatusMap.putIfAbsent(fileStatus,
				Long.valueOf(System.currentTimeMillis()))) != null) {
			// throw error
			throw new ConcurrencyException(fileStatus.getAgentName() + " "
					+ fileStatus.getLogType() + " " + fileStatus.getFileName()
					+ " is in processed by another thread with ts: " + ts);
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

}
