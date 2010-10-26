package org.streams.gring.message;

import org.streams.gring.group.GRingSnapshot;

public interface Message {

	enum TYPE { DISCOVER, WRITE }
	
	MessageTransmitListener getMessageTransmitListener();
	MessageReceiptListener getMessageReceiptListener();
	
	long getMessageId();
	TYPE getMessageType();
	
	GRingSnapshot getGRing();
	
}
