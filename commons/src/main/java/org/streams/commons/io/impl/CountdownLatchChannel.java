package org.streams.commons.io.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * 
 * This ChannelHandler used a CoundownLatch to allow another thread to block
 * until the channelClosed method has been called.
 * 
 */
public class CountdownLatchChannel extends SimpleChannelHandler {

	CountDownLatch closeLatch;

	public CountdownLatchChannel() {
		closeLatch = new CountDownLatch(1);
	}

	public CountdownLatchChannel(CountDownLatch closeLatch) {
		super();
		this.closeLatch = closeLatch;
	}

	/**
	 * Will call the CountDownLatch.await method
	 * 
	 * @param timeout
	 * @param timeUnit
	 * @throws InterruptedException
	 */
	public void waitTillClose(long timeout, TimeUnit timeUnit)
			throws InterruptedException {
		closeLatch.await(timeout, timeUnit);
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {

		closeLatch.countDown();
		super.channelClosed(ctx, e);
	}

}
