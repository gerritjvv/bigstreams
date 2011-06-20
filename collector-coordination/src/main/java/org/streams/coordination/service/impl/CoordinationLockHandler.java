package org.streams.coordination.service.impl;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Date;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusKey;
import org.streams.commons.file.SyncPointer;
import org.streams.coordination.file.FileTrackerStorage;
import org.streams.coordination.file.history.FileTrackerHistoryItem;
import org.streams.coordination.file.history.FileTrackerHistoryMemory;
import org.streams.coordination.mon.CoordinationStatus;
import org.streams.coordination.mon.CoordinationStatus.STATUS;
import org.streams.coordination.service.LockMemory;

/**
 * A netty handler to recieve file lock requests<br/>
 * The protocol expected is mesg len, FileTrackingStatus json.<br/>
 * the msg length is read by the Frame Decoder.<br/>
 * <p/>
 * Response:<br/>
 * 4 bytes syncpointer length + 4 bytes code | 4 bytes code | SyncPointer json
 * string
 * 
 */
public class CoordinationLockHandler extends SimpleChannelHandler {

	private final static Logger LOG = Logger
			.getLogger(CoordinationLockHandler.class);

	private static final byte[] CONFLICT_MESSAGE = "The resource is already locked"
			.getBytes();

	private static final ObjectMapper objMapper = new ObjectMapper();

	long lockTimeOut;

	CoordinationStatus coordinationStatus;
	LockMemory lockMemory;
	FileTrackerStorage memory;

	FileTrackerHistoryMemory fileTrackerHistoryMemory;

	/**
	 * The port to use for pinging for lock holders
	 */
	int pingPort;

	public CoordinationLockHandler(CoordinationStatus coordinationStatus,
			LockMemory lockMemory, FileTrackerStorage memory, int pingPort,
			long lockTimeout, FileTrackerHistoryMemory fileTrackerHistoryMemory) {
		this.coordinationStatus = coordinationStatus;
		this.lockMemory = lockMemory;
		this.memory = memory;
		this.pingPort = pingPort;
		this.lockTimeOut = lockTimeout;
		this.fileTrackerHistoryMemory = fileTrackerHistoryMemory;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		ChannelBuffer buff = (ChannelBuffer) e.getMessage();

		ChannelBufferInputStream channelInput = new ChannelBufferInputStream(
				buff);

		final FileTrackingStatus fileStatus = objMapper.readValue(channelInput,
				FileTrackingStatus.class);

		SocketAddress remoteAddressObj = e.getRemoteAddress();
		String collectorAddress = (remoteAddressObj == null) ? "unknown"
				: remoteAddressObj.toString();

		// NOTE: Correct usage for thread correctness is important here.
		// The first thing we MUST do here is try to attain a SyncPointer Lock
		// before doing anything else.
		final SyncPointer syncPointer = getAndLockResource(fileStatus,
				(InetSocketAddress) e.getRemoteAddress());

		try {
			ChannelBuffer buffer = null;

			if (syncPointer == null) {
				// if the sync pointer is null the resource is already locked
				buffer = ChannelBuffers.buffer(CONFLICT_MESSAGE.length + 8);

				buffer.writeInt(CONFLICT_MESSAGE.length + 4);
				buffer.writeInt(409); // conflict code
				buffer.writeBytes(CONFLICT_MESSAGE);

				// ----- Add FileTracking History
				fileTrackerHistoryMemory
						.addAsyncToHistory(new FileTrackerHistoryItem(
								new Date(), fileStatus.getAgentName(),
								collectorAddress,
								FileTrackerHistoryItem.STATUS.ALREADY_LOCKED));

			} else {
				// if a syncpointer is returned the resource was not locked and
				// is
				// now locked for the current caller.

				String msg = objMapper.writeValueAsString(syncPointer);
				byte[] msgBytes = msg.getBytes();

				int msgLen = msgBytes.length + 4;

				// 4 bytes == code, 4 bytes content length, rest is the message
				buffer = ChannelBuffers.dynamicBuffer(msgLen);
				buffer.writeInt(msgLen);
				buffer.writeInt(200);
				buffer.writeBytes(msgBytes, 0, msgBytes.length);

				// ------- Add FileTracking History
				FileTrackerHistoryItem.STATUS status = (syncPointer
						.getFilePointer() == fileStatus.getFilePointer()) ? FileTrackerHistoryItem.STATUS.OK
						: FileTrackerHistoryItem.STATUS.OUTOF_SYNC;

				fileTrackerHistoryMemory
						.addAsyncToHistory(new FileTrackerHistoryItem(
								new Date(), fileStatus.getAgentName(),
								collectorAddress, status));
				// ------- Finish History add

				if (LOG.isDebugEnabled()) {
					LOG.debug("LOCK( " + syncPointer.getLockId() + ") - "
							+ fileStatus.getAgentName() + "."
							+ fileStatus.getLogType() + "."
							+ fileStatus.getFileName());
				}
			}

			ChannelFuture future = e.getChannel().write(buffer);
			future.addListener(ChannelFutureListener.CLOSE);
		} catch (Exception t) {
			// we catch any exception here to ensure that in case of an error we
			// do release the lock held if any optained
			LOG.error("ERROR MAKING LOCK " + fileStatus.getAgentName() + "."
					+ fileStatus.getLogType() + "." + fileStatus.getFileName());
			// re-throw the error
			throw t;
		}
	}

	/**
	 * If the syncpointer returned is null the resource was already locked.
	 * 
	 * @param requestFileStatus
	 * @param remoteAddress
	 * @return
	 * @throws InterruptedException
	 */
	private SyncPointer getAndLockResource(
			FileTrackingStatus requestFileStatus,
			InetSocketAddress remoteAddress) throws InterruptedException {

		// NOTE: Correct usage for thread correctness is important here.
		// The first thing we MUST do here is try to attain a SyncPointer Lock
		// before doing anything else.
		// If a SyncPointer Lock cannot be attained we return an error to the
		// client.
		long startTime = System.currentTimeMillis();

		// ---------- This line uses a semi lock free algorithm
		SyncPointer pointer = lockMemory.setLock(requestFileStatus,
				lockTimeOut, remoteAddress.getAddress().getHostAddress());
		// ---------- lock released. At this stage we either have a SyncPointer
		// lock or a null reference if the resource was locked already.

		if (pointer == null) {
			// apply lock wait, the way lock memory is applied we cannot wait on
			// the particular value becoming free.
			// so we retry 5 times within a second.
			for (int i = 0; i < 5; i++) {
				LOG.info("Retrying Lock: " + i + " of 5");
				// we try getting the lock
				pointer = lockMemory.setLock(requestFileStatus, lockTimeOut,
						remoteAddress.getAddress().getHostAddress());

				if (pointer != null) {
					break;
				}
				// sleep for 200 milliseconds
				Thread.sleep(200L);
			}
		}

		long endTime = System.currentTimeMillis() - startTime;
		if (endTime >= 1000L) {
			LOG.warn("The coordination service lock appears to be running slow please check the network settings");
		}
		if (pointer == null) {
			coordinationStatus.incCounter("LOCK_CONFLICT_BLOCKED", 1);
			return null;
		} else {

			// The MAP will already be configured with a persistence
			// implementation to
			// perform the below
			FileTrackingStatus fileStatus = memory
					.getStatus(new FileTrackingStatusKey(requestFileStatus));

			if (fileStatus == null) {
				fileStatus = requestFileStatus;
				memory.setStatus(fileStatus);
			}

			coordinationStatus.incCounter("LOCKS", 1);
			coordinationStatus.setStatus(STATUS.OK, "running");
			return pointer;

		}

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		coordinationStatus.setStatus(STATUS.UNKOWN_ERROR, e.toString());
		Throwable error = e.getCause();
		LOG.error(e.toString(), error);

		e.getChannel().close();
	}

	// /**
	// * Pings the lock holder on the address /lockPing and checks the response
	// * code.<br/>
	// * If the code is ok then true is returned else false is returned.
	// *
	// * @param requestFileStatus
	// * @return
	// * @throws Exception
	// */
	// private boolean _pingLockHolder(FileTrackingStatus requestFileStatus)
	// throws Exception {
	//
	// boolean ret = false;
	// String msg = null;
	//
	// String holderAddress = lockHolders.get(requestFileStatus);
	//
	// if (holderAddress != null) {
	// holderAddress = "http://" + holderAddress + ":" + pingPort + "/";
	//
	// // do a simple get operation
	// // and check status
	// Client client = new Client(Protocol.HTTP);
	// try {
	// client.start();
	// Response rep = client.get(new Reference(holderAddress));
	//
	// ret = rep.getStatus().isSuccess();
	// msg = rep.getEntityAsText();
	//
	// } catch (Throwable t) {
	// t.printStackTrace();
	// // ignore any exceptions
	// msg = "failed " + t.toString();
	// } finally {
	// client.stop();
	// }
	// }
	//
	// Log.info("Lock holder " + holderAddress + " ping " + msg);
	// return ret;
	// }

	public CoordinationStatus getCoordinationStatus() {
		return coordinationStatus;
	}

	public void setCoordinationStatus(CoordinationStatus coordinationStatus) {
		this.coordinationStatus = coordinationStatus;
	}

	public LockMemory getLockMemory() {
		return lockMemory;
	}

	public void setLockMemory(LockMemory lockMemory) {
		this.lockMemory = lockMemory;
	}

	public FileTrackerStorage getMemory() {
		return memory;
	}

	public void setMemory(FileTrackerStorage memory) {
		this.memory = memory;
	}

	public int getPingPort() {
		return pingPort;
	}

	/**
	 * @Inject The port to use for pinging for lock holders
	 * @param pingPort
	 */
	public void setPingPort(int pingPort) {
		this.pingPort = pingPort;
	}

	public long getLockTimeOut() {
		return lockTimeOut;
	}

	public void setLockTimeOut(long lockTimeOut) {
		this.lockTimeOut = lockTimeOut;
	}

}
