package org.streams.gring.message.impl;

import org.jboss.netty.buffer.ChannelBuffer;
import org.streams.gring.group.GRingSnapshot;
import org.streams.gring.message.Message;
import org.streams.gring.message.MessageReceiptListener;
import org.streams.gring.message.MessageTransmitListener;

/**
 * 
 * Message identity is based only on the messageId.
 * 
 */
public class MessageImpl implements Message {

	transient MessageTransmitListener messageTransmitListener;
	transient MessageReceiptListener messageReceiptListener;

	long messageId;

	TYPE messageType;

	GRingSnapshot gring;

	ChannelBuffer dataBuffer;

	public MessageImpl(long messageId, TYPE messageType, GRingSnapshot gring,
			MessageReceiptListener messageReceiptListener,
			MessageTransmitListener messageTransmitListener) {
		super();
		this.messageId = messageId;
		this.messageType = messageType;
		this.gring = gring;
		this.messageReceiptListener = messageReceiptListener;
		this.messageTransmitListener = messageTransmitListener;
	}

	public GRingSnapshot getGRing() {
		return gring;
	}

	public TYPE getMessageType() {
		return messageType;
	}

	@Override
	public MessageTransmitListener getMessageTransmitListener() {
		return messageTransmitListener;
	}

	@Override
	public MessageReceiptListener getMessageReceiptListener() {
		return messageReceiptListener;
	}

	@Override
	public long getMessageId() {
		return messageId;
	}

	public ChannelBuffer getDataBuffer() {
		return dataBuffer;
	}

	public void setDataBuffer(ChannelBuffer dataBuffer) {
		this.dataBuffer = dataBuffer;
	}

	@Override
	public int compareTo(Message o) {
		long oId = o.getMessageId();

		if (messageId < oId) {
			return -1;
		} else if (messageId > oId) {
			return 1;
		} else {
			return 0;
		}

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (messageId ^ (messageId >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MessageImpl other = (MessageImpl) obj;
		if (messageId != other.messageId)
			return false;
		return true;
	}

	public String toString() {
		return "Message[id: " + messageId + "; type: " + messageType + "]";
	}

}
