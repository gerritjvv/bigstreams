package org.streams.coordination.service.impl;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;

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
import org.streams.commons.file.SyncPointer;
import org.streams.coordination.file.CollectorFileTrackerMemory;
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

	private static final byte[] CONFLICT_MESSAGE = "The resource is already locked"
			.getBytes();

	private static final ObjectMapper objMapper = new ObjectMapper();

	long lockTimeOut = 10000L;
	
	CoordinationStatus coordinationStatus;
	LockMemory lockMemory;
	CollectorFileTrackerMemory memory;
	/**
	 * The port to use for pinging for lock holders
	 */
	int pingPort;

	Map<FileTrackingStatus, String> lockHolders = new ConcurrentHashMap<FileTrackingStatus, String>();
	
    
	public CoordinationLockHandler() {
	}

	public CoordinationLockHandler(CoordinationStatus coordinationStatus,
			LockMemory lockMemory, CollectorFileTrackerMemory memory,
			int pingPort) {
		super();
		this.coordinationStatus = coordinationStatus;
		this.lockMemory = lockMemory;
		this.memory = memory;
		this.pingPort = pingPort;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		ChannelBuffer buff = (ChannelBuffer) e.getMessage();

		ChannelBufferInputStream channelInput = new ChannelBufferInputStream(
				buff);

		FileTrackingStatus fileStatus = objMapper.readValue(channelInput,
				FileTrackingStatus.class);

		SyncPointer syncPointer = getAndLockResource(fileStatus,
				(InetSocketAddress) e.getRemoteAddress());

		ChannelBuffer buffer = null;

		if (syncPointer == null) {
			// if the sync pointer is null the resource is already locked
			buffer = ChannelBuffers.buffer(CONFLICT_MESSAGE.length + 8);

			buffer.writeInt(CONFLICT_MESSAGE.length + 4);
			buffer.writeInt(409); // conflict code
			buffer.writeBytes(CONFLICT_MESSAGE);

		} else {
			// if a syncpointer is returned the resource was not locked and is
			// now locked for the current caller.

			String msg = objMapper.writeValueAsString(syncPointer);
			byte[] msgBytes = msg.getBytes();

			int msgLen = msgBytes.length + 4;

			// 4 bytes == code, 4 bytes content length, rest is the message
			buffer = ChannelBuffers.dynamicBuffer(msgLen);
			buffer.writeInt(msgLen);
			buffer.writeInt(200);
			buffer.writeBytes(msgBytes, 0, msgBytes.length);

		}

		ChannelFuture future = e.getChannel().write(buffer);
		future.addListener(ChannelFutureListener.CLOSE);
	}

	/**
	 * If the syncpointer returned is null the resource was already locked.
	 * 
	 * @param requestFileStatus
	 * @param remoteAddress
	 * @return
	 */
	private SyncPointer getAndLockResource(
			FileTrackingStatus requestFileStatus,
			InetSocketAddress remoteAddress) {

		if (lockMemory.contains(requestFileStatus)
				&& isLockValid(requestFileStatus)) {
			// return null
			coordinationStatus.incCounter("LOCK_CONFLICT_BLOCKED", 1);

			return null;
		} else {
			
			FileTrackingStatus fileStatus = memory.getStatus(
					requestFileStatus.getAgentName(),
					requestFileStatus.getLogType(),
					requestFileStatus.getFileName());

			if (fileStatus == null) {
				fileStatus = requestFileStatus;

				memory.setStatus(fileStatus);
			}

			
			SyncPointer pointer = null;
			// set lock
			// we synchronize here to be absolutelly sure that no other thread
			// has sneaked to this line.
			pointer = lockMemory.setLock(fileStatus);

			// save the address for the holder
			lockHolders.put(fileStatus, remoteAddress.getHostName());

			coordinationStatus.incCounter("LOCKS", 1);
			coordinationStatus.setStatus(STATUS.OK, "running");
			return pointer;

		}

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		coordinationStatus.setStatus(STATUS.UNKOWN_ERROR, e.toString());
		e.getChannel().close();
	}

	private boolean isLockValid(FileTrackingStatus requestFileStatus) {

		boolean isValid = false;
		
		try {
			
			long timeStamp = lockMemory.lockTimeStamp(requestFileStatus);
			
			isValid = ! (timeStamp == 0L || (System.currentTimeMillis()-timeStamp) > lockTimeOut );
			

		} catch (Exception e) {
			RuntimeException rte = new RuntimeException(e.toString(), e);
			rte.setStackTrace(e.getStackTrace());
			throw rte;
		}

		return isValid;
	}

//	/**
//	 * Pings the lock holder on the address /lockPing and checks the response
//	 * code.<br/>
//	 * If the code is ok then true is returned else false is returned.
//	 * 
//	 * @param requestFileStatus
//	 * @return
//	 * @throws Exception
//	 */
//	private boolean _pingLockHolder(FileTrackingStatus requestFileStatus)
//			throws Exception {
//
//		boolean ret = false;
//		String msg = null;
//
//		String holderAddress = lockHolders.get(requestFileStatus);
//
//		if (holderAddress != null) {
//			holderAddress = "http://" + holderAddress + ":" + pingPort + "/";
//
//			// do a simple get operation
//			// and check status
//			Client client = new Client(Protocol.HTTP);
//			try {
//				client.start();
//				Response rep = client.get(new Reference(holderAddress));
//
//				ret = rep.getStatus().isSuccess();
//				msg = rep.getEntityAsText();
//
//			} catch (Throwable t) {
//				t.printStackTrace();
//				// ignore any exceptions
//				msg = "failed " + t.toString();
//			} finally {
//				client.stop();
//			}
//		}
//
//		Log.info("Lock holder " + holderAddress + " ping " + msg);
//		return ret;
//	}

	public CoordinationStatus getCoordinationStatus() {
		return coordinationStatus;
	}

	@Inject
	public void setCoordinationStatus(CoordinationStatus coordinationStatus) {
		this.coordinationStatus = coordinationStatus;
	}

	public LockMemory getLockMemory() {
		return lockMemory;
	}

	@Inject
	public void setLockMemory(LockMemory lockMemory) {
		this.lockMemory = lockMemory;
	}

	public CollectorFileTrackerMemory getMemory() {
		return memory;
	}

	@Inject
	public void setMemory(CollectorFileTrackerMemory memory) {
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
