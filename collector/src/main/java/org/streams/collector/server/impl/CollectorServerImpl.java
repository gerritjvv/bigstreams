package org.streams.collector.server.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.MemoryAwareThreadPoolExecutor;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.mortbay.log.Log;
import org.streams.collector.conf.CollectorProperties;
import org.streams.collector.server.CollectorServer;
import org.streams.commons.file.CoordinationServiceClient;
import org.streams.commons.file.impl.MessageFrameDecoder;
import org.streams.commons.util.HashedWheelTimerFactory;

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
	long readTimeout = 10000L;

	Configuration conf;

	IpFilterHandler ipFilterHandler;

	ExecutorService workerService;
	ExecutorService workerbossService;
	private NioServerSocketChannelFactory channelFactory;

	public CollectorServerImpl(int port, ChannelHandler channelHandler,
			Configuration conf, ChannelHandler metricsHandler,
			IpFilterHandler ipFilterHandler) {
		super();
		this.port = port;
		this.channelHandler = channelHandler;
		this.conf = conf;
		this.metricsHandler = metricsHandler;
		this.ipFilterHandler = ipFilterHandler;
	}

	@Override
	public void connect() {

		workerService = createWorkerService(getThreadPoolType(CollectorProperties.WRITER.COLLECTOR_WORKER_THREAD_POOL));

		workerbossService = createWorkderBossService(getThreadPoolType(CollectorProperties.WRITER.COLLECTOR_WORKERBOSS_THREAD_POOL));
		channelFactory = new NioServerSocketChannelFactory(workerbossService,
				workerService);

		bootstrap = new ServerBootstrap(channelFactory);

		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(ipFilterHandler,
						new MessageFrameDecoder(), new ReadTimeoutHandler(
								HashedWheelTimerFactory.getInstance(),
								readTimeout, TimeUnit.MILLISECONDS),
						metricsHandler, channelHandler);
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

		if (workerService != null) {
			Log.info("Shutdown worker threads");

			workerService.shutdown();
			try {
				workerService.awaitTermination(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				; // ignore we're shutting down
			}

			workerService.shutdownNow();

		}

		if (workerbossService != null) {
			Log.info("Shutdown manage threads");
			workerService.shutdown();
			try {
				workerService.awaitTermination(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				; // ignore we're shutting down
			}

			workerbossService.shutdownNow();
		}

		HashedWheelTimerFactory.shutdown();

		Log.info("Shutdown Collector Server");

	}

	public long getWriteTimeout() {
		return writeTimeout;
	}

	public void setWriteTimeout(long writeTimeout) {
		this.writeTimeout = writeTimeout;
	}

	public long getReadTimeout() {
		return readTimeout;
	}

	public void setReadTimeout(long readTimeout) {
		this.readTimeout = readTimeout;
	}

}
