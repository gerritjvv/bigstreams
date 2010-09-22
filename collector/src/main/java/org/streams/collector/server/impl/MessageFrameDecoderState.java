package org.streams.collector.server.impl;

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
