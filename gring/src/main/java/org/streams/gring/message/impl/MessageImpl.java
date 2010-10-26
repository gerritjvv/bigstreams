package org.streams.gring.message.impl;

import org.streams.gring.group.GRingSnapshot;
import org.streams.gring.message.Message;
import org.streams.gring.message.MessageReceiptListener;
import org.streams.gring.message.MessageTransmitListener;

public class MessageImpl implements Message{

	transient MessageTransmitListener messageTransmitListener;
	transient MessageReceiptListener messageReceiptListener;
	
	long messageId;
	
	TYPE messageType;
	
	GRingSnapshot gring;
	
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

}
