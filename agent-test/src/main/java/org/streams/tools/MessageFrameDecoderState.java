package org.streams.tools;

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
