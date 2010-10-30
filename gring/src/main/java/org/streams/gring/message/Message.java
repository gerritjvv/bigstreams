package org.streams.gring.message;

import org.jboss.netty.buffer.ChannelBuffer;
import org.streams.gring.group.GRingSnapshot;

public interface Message extends Comparable<Message> {

	public enum TYPE {
		DISCOVER((byte)0), WRITE((byte)1);
		
		byte identifier;
		TYPE(byte identifier){
			this.identifier = identifier;
		}
		
		public byte getIdentifier(){
			return identifier;
		}
		
		public TYPE getFromIdentifier(byte identifier){
			TYPE type = null;
			
			switch(identifier){
				case (byte)0: 
					type = DISCOVER;
				case (byte)1:
					type = WRITE;
				
			};
			
			return type;
		}
	}

	MessageTransmitListener getMessageTransmitListener();

	MessageReceiptListener getMessageReceiptListener();

	long getMessageId();

	TYPE getMessageType();

	GRingSnapshot getGRing();

	ChannelBuffer getDataBuffer();

	void setDataBuffer(ChannelBuffer dataBuffer);

}
