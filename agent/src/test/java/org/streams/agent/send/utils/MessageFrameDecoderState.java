package org.streams.agent.send.utils;

/**
 * 
 * Message decoding state
 * 
 */
public enum MessageFrameDecoderState {

	READ_LENGTH,
	READ_HEADER,
	READ_CONTENT;
	
}
