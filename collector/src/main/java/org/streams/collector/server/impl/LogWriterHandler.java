package org.streams.collector.server.impl;

import java.io.DataInputStream;
import java.io.IOException;
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
import org.streams.collector.write.PostWriteAction;
import org.streams.commons.compression.CompressionPool;
import org.streams.commons.compression.CompressionPoolFactory;
import org.streams.commons.file.CoordinationException;
import org.streams.commons.file.CoordinationServiceClient;
import org.streams.commons.file.FileTrackingStatus;
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
				new Date(), header.getFilePointer(), header.getFileSize(),
				header.getLinePointer(), agentName, fileName, logType,
				header.getFileDate(), System.currentTimeMillis());

		final AgentSession agentSession = (AgentSession) ctx.getAttachment();
		agentSession.setMessageReceived();
		agentSession.setAgentName(agentName);
		agentSession.setFileName(fileName);
		agentSession.setLogType(logType);

		// save the session to the context
		ctx.setAttachment(agentSession);

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

		final String localKey = fileStatus.getAgentName()
				+ fileStatus.getLogType() + fileStatus.getFileName();
		boolean localLockAcquired = false;

		try {

			// --------------------- Check Coordination parameters
			// --------------------- that is that the agent is not
			// sending
			// duplicate
			// data

			// We synchronise here on agent + log type + fileName to
			// prevent any
			// agent sending more than one request per file at a time.
			// during normal expected operation there will only ever be
			// one
			// agent
			// message per agent + fileName
			// but the collector has to guard against this possible
			// event.

			// SYNC LOCALLY
			// this local sync will cause possible faulty sends to be
			// caught but
			// will also
			// Synchronise concurrent calls from agents for the same
			// file.
			// This concurrentness is not allowed but offers a gracefull
			// exit as
			// any call out of sync will get a resync exception.
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
			final SyncPointer syncPointer = coordinationService
					.getAndLock(fileStatus);

			if (syncPointer == null) {
				collectorStatus.setStatus(
						CollectorStatus.STATUS.COORDINATION_ERROR,
						"File already Locked ERROR " + fileName);
				throw new CoordinationException("File already locked "
						+ fileName);
			}

			// register to session that the coordination lock could be acquired
			agentSession.setAcquiredCoordinationLock();

			if (LOG.isDebugEnabled()) {
				LOG.debug("LOCK(" + syncPointer.getLockId() + ")");
			}

			final long syncFilePointer = syncPointer.getFilePointer();
			final long filePointer = fileStatus.getFilePointer();

			ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();

			try { // try finally for pointer lock release

				if (syncFilePointer == filePointer) {

					try {
						// Note on rollback:
						// The writer will writer the file data, and
						// then
						// execute the PostWriteAction
						// if any step fails the file will be rolled
						// back.
						// This mean that if the syncPointer release
						// send to the
						// CoordinationService fails the file is rolled
						// back
						// and an error thrown.

						agentSession.setFileWriteStartTime();
						
						bytesWritten = writer.write(fileStatus, compressInput,
								new PostWriteAction() {

									@Override
									public void run(int bytesWritten)
											throws Exception {
										// INCREMENT FILE SYNCPOINTER
										syncPointer
												.incFilePointer(bytesWritten);
									}
								});

						// register that the file was written
						agentSession.setWrittenToFile();

						// check slow writes:
						if ((agentSession.getFileWriteEndTime() - agentSession
								.getFileWriteStartTime()) > 500) {
							LOG.error("File writting is slowing down please check the log directory");
						}

						
						buffer.writeInt(200);
						
						LOG.info("Written file data: " + (agentSession.getFileWriteEndTime() - agentSession
								.getFileWriteStartTime()));
						
						collectorStatus.setStatus(CollectorStatus.STATUS.OK,
								"Running");
						if (bytesWritten > -1) {
							// send kilobytes written
							fileBytesWrittenMetric
									.incrementCounter(bytesWritten / 1024);
						}
					} catch (Throwable t) {

						collectorStatus.setStatus(
								CollectorStatus.STATUS.UNKOWN_ERROR,
								t.toString());

						LOG.error(t.toString(), t);
						buffer.writeInt(500);
					} finally {
						pool.closeAndRelease(compressInput);
						compressInputWasReleased = true;
						IOUtils.closeQuietly(datInput);
						IOUtils.closeQuietly(channelInput);
					}

				} else {
					LOG.info("File pointer Conflict detected: agent "
							+ header.getHost() + " file: "
							+ header.getFileName() + " agentPointer: "
							+ header.getFilePointer() + " collectorPointer: "
							+ syncFilePointer);

					// send the sync pointer to the agent, this is a
					// request
					// made to
					// the
					// agent that is sends data starting from this
					// pointer
					// write the http codec 409 == Conflict
					buffer.writeInt(NetworkCodes.CODE.SYNC_CONFLICT.num());
					buffer.writeLong(syncFilePointer);
				}

			} finally {
				// release pointer lock
				sendSyncRelease(syncPointer);
				agentSession.setReleasedCoordinationLock();
			}

			future = e.getChannel().write(buffer);
			agentSession.setSentResponseRequest();

		} finally {
			// on any error event with coordination these resources must
			// be
			// released
			// assert fileStatusMap.remove(fileStatus) != null;
			if (localLockAcquired) {
				agentFileLocalLock.releaseLock(localKey);
			}

			if (!compressInputWasReleased) {
				pool.closeAndRelease(compressInput);
			}

		}

		if (future != null) {
			future.addListener(new ChannelFutureListener() {

				@Override
				public void operationComplete(ChannelFuture arg0)
						throws Exception {
					try {
						ChannelFutureListener.CLOSE.operationComplete(arg0);
					} catch (Throwable t) {
						LOG.error("ERROR While closing channel :"
								+ arg0.getChannel() + " " + arg0.getCause());
					}
				}

			});

		}

		// log the agent session
		if (LOG.isDebugEnabled()) {
			LOG.debug(agentSession.toString());
		}
	}

	/**
	 * Calls the saveAndFreeLock method on the coordination service.
	 * 
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
