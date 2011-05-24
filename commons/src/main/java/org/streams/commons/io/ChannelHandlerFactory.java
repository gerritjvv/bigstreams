package org.streams.commons.io;

import org.jboss.netty.channel.ChannelHandler;

/**
 * 
 * Creates a channel handler
 *
 */
public interface ChannelHandlerFactory {

	ChannelHandler create();
	
}
