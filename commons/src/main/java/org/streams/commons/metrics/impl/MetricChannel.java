package org.streams.commons.metrics.impl;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.streams.commons.metrics.CounterMetric;

/**
 * 
 * This channel supply the following metrics:<br/>
 * <ul>
 *   <li>Connections received per second</li>
 *   <li>Connections processed per second i.e. opened and closed</li>
 *   <li>Kilobytes received per second</li>
 *   <li>Kilobytes Written per second</li>
 *   <li>Errors per second</li>
 * </ul>
 */
public class MetricChannel extends SimpleChannelHandler{

	CounterMetric connectionsPerSecond;
	CounterMetric connectionsProcessedPerSecond;
	CounterMetric kiloBytesWrittenPerSecond;
	CounterMetric kiloBytesReceivedPerSecond;
	CounterMetric errorsPerSecond;
	
	
	/**
	 * 
	 * @param connectionsPerSecond
	 * @param connectionsProcessedPerSecond
	 * @param kiloBytesWrittenPerSecond
	 * @param kiloBytesReceivedPerSecond
	 * @param errorsPerSecond
	 */
	public MetricChannel(CounterMetric connectionsPerSecond,
			CounterMetric connectionsProcessedPerSecond,
			CounterMetric kiloBytesWrittenPerSecond,
			CounterMetric kiloBytesReceivedPerSecond,
			CounterMetric errorsPerSecond) {
		super();
		this.connectionsPerSecond = connectionsPerSecond;
		this.connectionsProcessedPerSecond = connectionsProcessedPerSecond;
		this.kiloBytesWrittenPerSecond = kiloBytesWrittenPerSecond;
		this.kiloBytesReceivedPerSecond = kiloBytesReceivedPerSecond;
		this.errorsPerSecond = errorsPerSecond;
	}


	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		errorsPerSecond.incrementCounter(1);
		super.exceptionCaught(ctx, e);
	}

	
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		connectionsPerSecond.incrementCounter(1);
		super.channelConnected(ctx, e);
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		connectionsProcessedPerSecond.incrementCounter(1);
		super.channelClosed(ctx, e);
	}

	@Override
	public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e)
			throws Exception {
		kiloBytesWrittenPerSecond.incrementCounter(e.getWrittenAmount()/1024);
		super.writeComplete(ctx, e);
	}


	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		final ChannelBuffer buff = (ChannelBuffer) e.getMessage();
		
		kiloBytesReceivedPerSecond.incrementCounter(buff.readableBytes()/1024);
		super.messageReceived(ctx, e);
	}

	
	
	
}
