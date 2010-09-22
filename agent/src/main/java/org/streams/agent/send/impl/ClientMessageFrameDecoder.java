package org.streams.agent.send.impl;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;

/**
 * 
 * Assures that the whole message sent from the collector has reached the client.
 */
public class ClientMessageFrameDecoder extends ReplayingDecoder<ClientDecoderState>{

	int code;
	
	public ClientMessageFrameDecoder(){
		checkpoint(ClientDecoderState.READ_RESPONSE);
	}
	
	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buffer, ClientDecoderState state) throws Exception {
		
		switch(state){
		
		case READ_RESPONSE:
			code = buffer.readInt();
			if(code == ClientHandlerContext.STATUS_CONFLICT){
				checkpoint(ClientDecoderState.READ_LONG);
			}else{
				ChannelBuffer frame = ChannelBuffers.buffer(4);
				frame.writeInt(code);
				return frame;
			}
			
		case READ_LONG:
			
			long filePointer = buffer.readLong();
			ChannelBuffer frame = ChannelBuffers.buffer(12);
			frame.writeInt(code);
			frame.writeLong(filePointer);
			return frame;
			
		default:
			 throw new Error("Error decoding message from collector");
			 
		}
		
	}
	
}
