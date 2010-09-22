package org.streams.collector.server.impl;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;

/**
 * 
 * Assures that the whole message has been reached.
 */
public class MessageFrameDecoder extends ReplayingDecoder<MessageFrameDecoderState>{

	int length;
	
	public MessageFrameDecoder(){
		checkpoint(MessageFrameDecoderState.READ_LENGTH);
	}
	
	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buffer, MessageFrameDecoderState state) throws Exception {
		
		switch(state){
		
		case READ_LENGTH:
			length = buffer.readInt();
			checkpoint(MessageFrameDecoderState.READ_CONTENT);
		case READ_CONTENT:
			ChannelBuffer frame = buffer.readBytes(length);
			checkpoint(MessageFrameDecoderState.READ_LENGTH);
			return frame;
		default:
			 throw new Error("Error decoding message from client");
			 
		}
		
	}
	
}
