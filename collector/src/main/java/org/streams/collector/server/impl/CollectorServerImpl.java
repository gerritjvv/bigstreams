package org.streams.collector.server.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.configuration.Configuration;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.MemoryAwareThreadPoolExecutor;
import org.streams.collector.conf.CollectorProperties;
import org.streams.collector.server.CollectorServer;
import org.streams.commons.file.CoordinationServiceClient;
import org.streams.commons.file.impl.MessageFrameDecoder;

/**
 * 
 * Uses the netty ServerBootstrap to bind a Server Socket to the specified port.<br/>
 * A ChannelPipeline is created that uses a new instance for MessageFrameDecoder
 * and the same instance on every call of the ChannelHandler passed in the
 * constructor.<br/>
 * <p/>
 * Note that the ChannelHandler passed in the constructor must be Thread Safe.
 * 
 * 
 */
public class CollectorServerImpl implements CollectorServer {

	ServerBootstrap bootstrap;
	int port;

	ChannelHandler channelHandler;
	ChannelHandler metricsHandler;

	CoordinationServiceClient coordinationServiceClient;

	long writeTimeout = 10000L;

	Configuration conf;

	public CollectorServerImpl(int port, ChannelHandler channelHandler,
			CoordinationServiceClient coordinationServiceClient,
			Configuration conf, ChannelHandler metricHandler) {
		super();
		this.port = port;
		this.channelHandler = channelHandler;
		this.coordinationServiceClient = coordinationServiceClient;
		this.conf = conf;
		this.metricsHandler = metricHandler;
	}

	@Override
	public void connect() {

		ExecutorService workerService = createWorkerService(getThreadPoolType(CollectorProperties.WRITER.COLLECTOR_WORKER_THREAD_POOL));

		ExecutorService workerbossService = createWorkderBossService(getThreadPoolType(CollectorProperties.WRITER.COLLECTOR_WORKERBOSS_THREAD_POOL));

		bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				workerbossService, workerService));

		// we use a WriteTimeoutHandler to timeout if the agent fails to
		// respond.
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
//				ChannelPipeline p = Channels.pipeline();
//				p.addFirst("MessageFrameDecoder", new MessageFrameDecoder());
//				p.addLast("MetricsHandler", metricsHandler);
//				p.addLast("ChannelHandler", channelHandler);
//				return p;
				 return Channels.pipeline(new MessageFrameDecoder(),
				 metricsHandler,
				 channelHandler);
			}
		});

		bootstrap.bind(new InetSocketAddress(port));

	}

	private THREAD_POOLS getThreadPoolType(CollectorProperties.WRITER property) {

		String type = conf.getString(property.toString(),
				(String) property.getDefaultValue());

		THREAD_POOLS poolType;

		try {
			poolType = THREAD_POOLS.valueOf(type.toUpperCase());
		} catch (IllegalArgumentException illArg) {
			poolType = THREAD_POOLS.CACHED;
		}

		return poolType;
	}

	private ExecutorService createWorkderBossService(THREAD_POOLS poolType) {

		ExecutorService service;

		switch (poolType) {

		case FIXED:
			service = createFixedPool(CollectorProperties.WRITER.COLLECTOR_WORKERBOSS_THREAD_COUNT);
			break;

		default: // for memory and cached return a cached thread pool
			service = Executors.newCachedThreadPool();
			break;
		}

		return service;
	}

	private ExecutorService createWorkerService(THREAD_POOLS poolType) {

		ExecutorService service;

		switch (poolType) {

		case FIXED:
			service = createFixedPool(CollectorProperties.WRITER.COLLECTOR_WORKER_THREAD_COUNT);
			break;

		case MEMORY:
			service = createMemoryAwarePool();
			break;
		default:
			service = Executors.newCachedThreadPool();
			break;
		}

		return service;
	}

	private ExecutorService createMemoryAwarePool() {

		int corePoolSize = conf
				.getInt(CollectorProperties.WRITER.COLLECTOR_WORKER_THREAD_COUNT
						.toString(),
						(Integer) CollectorProperties.WRITER.COLLECTOR_WORKER_THREAD_COUNT
								.getDefaultValue());

		long maxChannelMemorySize = conf
				.getLong(
						CollectorProperties.WRITER.COLLECTOR_CHANNEL_MAX_MEMORY_SIZE
								.toString(),
						(Long) CollectorProperties.WRITER.COLLECTOR_CHANNEL_MAX_MEMORY_SIZE
								.getDefaultValue());

		long maxTotalMemorySize = conf.getLong(
				CollectorProperties.WRITER.COLLECTOR_TOTAL_MEMORY_SIZE
						.toString(),
				(Long) CollectorProperties.WRITER.COLLECTOR_TOTAL_MEMORY_SIZE
						.getDefaultValue());

		return new MemoryAwareThreadPoolExecutor(corePoolSize,
				maxChannelMemorySize, maxTotalMemorySize);

	}

	private ExecutorService createFixedPool(
			CollectorProperties.WRITER countProperty) {

		int fixedCount = conf.getInt(countProperty.toString(),
				(Integer) countProperty.getDefaultValue());

		return Executors.newFixedThreadPool(fixedCount);

	}

	@Override
	public void shutdown() {
		if (bootstrap != null) {
			bootstrap.releaseExternalResources();
		}

		if (coordinationServiceClient != null) {
			coordinationServiceClient.destroy();
		}

	}

	public long getWriteTimeout() {
		return writeTimeout;
	}

	public void setWriteTimeout(long writeTimeout) {
		this.writeTimeout = writeTimeout;
	}

}
