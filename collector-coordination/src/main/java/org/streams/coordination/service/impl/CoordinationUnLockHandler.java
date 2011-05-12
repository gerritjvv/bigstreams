package org.streams.coordination.service.impl;

import java.net.InetSocketAddress;
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
import org.restlet.resource.Put;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.SyncPointer;
import org.streams.coordination.file.FileTrackerStorage;
import org.streams.coordination.mon.CoordinationStatus;
import org.streams.coordination.mon.CoordinationStatus.STATUS;
import org.streams.coordination.service.LockMemory;

/**
 * A netty handler to recieve file unlock requests<br/>
 * The protocol expected is mesg len, SyncPointer json.<br/>
 * the msg length is read by the Frame Decoder.<br/>
 * <p/>
 * Response:<br/>
 * 4 bytes msg length + 4 bytes from code | 4 bytes code | msg string
 * 
 */
public class CoordinationUnLockHandler extends SimpleChannelHandler {

	private final static Logger LOG = Logger
			.getLogger(CoordinationUnLockHandler.class);

	private static final byte[] CONFLICT_MESSAGE = "The resource was not locked"
			.getBytes();

	private static final ObjectMapper objMapper = new ObjectMapper();

	CoordinationStatus coordinationStatus;
	LockMemory lockMemory;
	FileTrackerStorage memory;

	public CoordinationUnLockHandler() {
	}

	public CoordinationUnLockHandler(CoordinationStatus coordinationStatus,
			LockMemory lockMemory, FileTrackerStorage memory) {
		super();
		this.coordinationStatus = coordinationStatus;
		this.lockMemory = lockMemory;
		this.memory = memory;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		ChannelBuffer buff = (ChannelBuffer) e.getMessage();

		ChannelBufferInputStream channelInput = new ChannelBufferInputStream(
				buff);

		final SyncPointer syncPointer = objMapper.readValue(channelInput,
				SyncPointer.class);

		if (syncPointer == null) {
			throw new RuntimeException(
					"Please send a SyncPointer object: SyncPointer is null");
		}

		if (syncPointer.getLockId() == null) {
			throw new RuntimeException(
					"Please send a SyncPointer object with a lockId: SyncPointer.getLockId == null");
		}
		
		final boolean ok = saveAndReleaseLock(syncPointer, (InetSocketAddress) e.getRemoteAddress());

		ChannelBuffer buffer = null;

		if (ok) {
			// if a syncpointer is returned the resource was not locked and is
			// now locked for the current caller.

			byte[] okBytes = "OK".getBytes();
			buffer = ChannelBuffers.buffer(okBytes.length + 8);
			buffer.writeInt(okBytes.length + 4);
			buffer.writeInt(200);
			buffer.writeBytes(okBytes);

		} else {
			// if the sync pointer is null the resource is already locked
			buffer = ChannelBuffers.buffer(CONFLICT_MESSAGE.length + 8);

			buffer.writeInt(CONFLICT_MESSAGE.length + 4);
			buffer.writeInt(409); // conflict code
			buffer.writeBytes(CONFLICT_MESSAGE);

		}

		ChannelFuture future = e.getChannel().write(buffer);
		future.addListener(ChannelFutureListener.CLOSE);
	}

	/**
	 * The SyncPointer lock will be removed and the file pointer, and file size
	 * set by the syncPointer will be saved to the FileTrackingStatus.
	 * 
	 * @param syncPointer
	 * @return false if the resource wasn't locked true if the resource was
	 *         locked and is now released
	 * @throws InterruptedException
	 */
	@Put("json")
	public boolean saveAndReleaseLock(SyncPointer syncPointer, InetSocketAddress remoteAddress)
			throws InterruptedException {

		// NOTE: Correct usage for thread correctness is important here.
		// The first thing we MUST do here is try to release a SyncPointer Lock
		// before doing anything else.
		FileTrackingStatus fileStatus = lockMemory.removeLock(syncPointer, remoteAddress.getAddress().getHostAddress());

		if (fileStatus == null) {
			LOG.error("ERROR UNLOCK(" + syncPointer.getLockId() + ")");
			return false;
		} else {
			if(LOG.isDebugEnabled()){
			LOG.debug("UNLOCK(" + syncPointer.getLockId() + ") - "
					+ fileStatus.getAgentName() + "." + fileStatus.getLogType()
					+ "." + fileStatus.getFileName());
			}
			
			fileStatus.setFilePointer(syncPointer.getFilePointer());
			fileStatus.setFileSize(syncPointer.getFileSize());
			LOG.info(">>>>>: " + syncPointer.getLinePointer());
			fileStatus.setLinePointer(syncPointer.getLinePointer());
			fileStatus.setDate(new Date());
			
			memory.setStatus(fileStatus);

			coordinationStatus.decCounter("LOCKS", 1);
		}

		coordinationStatus.setStatus(STATUS.OK, "running");

		return true;

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		LOG.error(e.toString(), e.getCause());

		coordinationStatus.setStatus(STATUS.UNKOWN_ERROR, e.toString());

		e.getChannel().close();
	}

}
