package org.streams.coordination.service.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.streams.commons.file.impl.MessageFrameDecoder;
import org.streams.coordination.service.CoordinationServer;

/**
 * Starts and stops the main coordination server for handling lock and release
 * requests
 * 
 */
public class CoordinationServerImpl implements CoordinationServer {

	ServerBootstrap lockBootstrap;
	ServerBootstrap unlockBootstrap;

	int lockPort;
	int releaseLockPort;

	ChannelHandler lockHandler;
	ChannelHandler unlockHandler;
	ChannelHandler metricHandler;

	public CoordinationServerImpl(int lockPort, int releaseLockPort,
			ChannelHandler lockHandler, ChannelHandler unlockHandler,
			ChannelHandler metricHandler) {
		super();
		this.lockPort = lockPort;
		this.releaseLockPort = releaseLockPort;
		this.lockHandler = lockHandler;
		this.unlockHandler = unlockHandler;
		this.metricHandler = metricHandler;
	}

	public void connect() {
		connectLockBootstrap();
		connectUnlockBootstrap();
	}

	public void shutdown() {
		lockBootstrap.releaseExternalResources();
		unlockBootstrap.releaseExternalResources();
	}

	/**
	 * Startup a ServerBootstrap with NioServerSocketChannelFactory using the
	 * portNo specified in the constructor.
	 * 
	 */
	private void connectLockBootstrap() {

		lockBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		lockBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
//				ChannelPipeline p = Channels.pipeline();
//				p.addFirst("MessageFrameDecoder", new MessageFrameDecoder());
//				p.addLast("MetricHandler", metricHandler);
//				p.addLast("LockHandler", lockHandler);
				return Channels.pipeline(new MessageFrameDecoder(), metricHandler,
				 lockHandler);
//				return p;
			}
		});

		lockBootstrap.bind(new InetSocketAddress(lockPort));

	}

	/**
	 * Startup a ServerBootstrap with NioServerSocketChannelFactory using the
	 * portNo specified in the constructor.
	 * 
	 */
	private void connectUnlockBootstrap() {

		unlockBootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		unlockBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
//				ChannelPipeline p = Channels.pipeline();
//				p.addFirst("MessageFrameDecoder", new MessageFrameDecoder());
//				p.addLast("UnLockHandler", unlockHandler);
//				return p;
				 return Channels.pipeline(new MessageFrameDecoder(),
				 unlockHandler);
			}
		});

		unlockBootstrap.bind(new InetSocketAddress(releaseLockPort));

	}
}
