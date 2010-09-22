package org.streams.coordination.service.impl;

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
import org.streams.coordination.file.CollectorFileTrackerMemory;
import org.streams.coordination.mon.CoordinationStatus;
import org.streams.coordination.mon.CoordinationStatus.STATUS;
import org.streams.coordination.service.LockMemory;


/**
 * A netty handler to recieve file unlock requests<br/>
 * The protocol expected is mesg len, SyncPointer json.<br/>
 * the msg length is read by the Frame Decoder.<br/>
 * <p/>
 * Response:<br/>
 * 4 bytes msg length + 4 bytes from code | 4 bytes code  | msg string
 * 
 */
public class CoordinationUnLockHandler extends SimpleChannelHandler {

	private static final byte[] CONFLICT_MESSAGE = "The resource was not locked"
			.getBytes();

	private static final ObjectMapper objMapper = new ObjectMapper();

	CoordinationStatus coordinationStatus;
	LockMemory lockMemory;
	CollectorFileTrackerMemory memory;

	public CoordinationUnLockHandler() {
	}

	public CoordinationUnLockHandler(CoordinationStatus coordinationStatus,
			LockMemory lockMemory, CollectorFileTrackerMemory memory) {
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

		
		SyncPointer syncPointer = objMapper.readValue(channelInput,
				SyncPointer.class);

		if(syncPointer == null){
			throw new RuntimeException("Please send a SyncPointer object: SyncPointer is null");
		}
		
		boolean ok = saveAndeleaseLock(syncPointer);

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
	 */
	@Put("json")
	public boolean saveAndeleaseLock(SyncPointer syncPointer) {
		try {
			FileTrackingStatus fileStatus = lockMemory.removeLock(syncPointer);

			if (fileStatus == null) {
				return false;
			} else {
				fileStatus.setFilePointer(syncPointer.getFilePointer());
				fileStatus.setFileSize(syncPointer.getFileSize());
				fileStatus.setLinePointer(syncPointer.getLinePointer());

				memory.setStatus(fileStatus);

			}

			coordinationStatus.decCounter("LOCKS", 1);
			coordinationStatus.setStatus(STATUS.OK, "running");

			return true;
		} catch (Throwable t) {
			coordinationStatus.setStatus(STATUS.UNKOWN_ERROR, t.toString());
			RuntimeException tre = new RuntimeException(t.toString(), t);
			tre.setStackTrace(t.getStackTrace());
			throw tre;
		}

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		coordinationStatus.setStatus(STATUS.UNKOWN_ERROR, e.toString());
		e.getChannel().close();
	}

}
