package org.streams.agent.send.utils;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.MessageEvent;

/**
 * Helper class that encapsulates the bytes received in each mesageReceived call from either the server or client.
 *
 */
public class MessageEventBag {
	byte[] bytes;

	public byte[] getBytes() {
		return bytes;
	}

	public void setBytes(MessageEvent event) {
		Object message = event.getMessage();
		ChannelBuffer buff = (ChannelBuffer) message;

		bytes = buff.array();
	}
}
