package org.streams.collector.di.impl;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.Restlet;
import org.restlet.resource.Finder;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;
import org.restlet.routing.Template;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.streams.collector.cli.startup.check.impl.CodecCheck;
import org.streams.collector.cli.startup.check.impl.ConfigCheck;
import org.streams.collector.cli.startup.check.impl.PingCheck;
import org.streams.collector.conf.CollectorProperties;
import org.streams.collector.coordination.impl.CoordinationAddresses;
import org.streams.collector.mon.CollectorStatus;
import org.streams.collector.mon.impl.AgentsStatusResource;
import org.streams.collector.mon.impl.CollectorConfigResource;
import org.streams.collector.mon.impl.CollectorStatusResource;
import org.streams.collector.mon.impl.CollectorsStatusResource;
import org.streams.collector.mon.impl.PingOKResource;
import org.streams.collector.server.CollectorServer;
import org.streams.collector.server.impl.CollectorServerImpl;
import org.streams.collector.server.impl.IpFilterHandler;
import org.streams.collector.server.impl.LogWriterHandler;
import org.streams.collector.write.FileOutputStreamPoolFactory;
import org.streams.collector.write.LogFileNameExtractor;
import org.streams.collector.write.LogFileWriter;
import org.streams.collector.write.LogRollover;
import org.streams.collector.write.LogRolloverCheck;
import org.streams.collector.write.OrphanedFilesCheck;
import org.streams.collector.write.impl.DateHourFileNameExtractor;
import org.streams.collector.write.impl.OrphanedFilesCheckImpl;
import org.streams.commons.app.AppLifeCycleManager;
import org.streams.commons.app.ApplicationService;
import org.streams.commons.app.StartupCheck;
import org.streams.commons.app.impl.AppLifeCycleManagerImpl;
import org.streams.commons.app.impl.AppShutdownResource;
import org.streams.commons.app.impl.RestletService;
import org.streams.commons.cli.AppStartCommand;
import org.streams.commons.compression.CompressionPoolFactory;
import org.streams.commons.file.CoordinationServiceClient;
import org.streams.commons.file.impl.CoordinationServiceClientImpl;
import org.streams.commons.group.Group;
import org.streams.commons.group.GroupHeartbeatService;
import org.streams.commons.group.GroupKeeper;
import org.streams.commons.io.Protocol;
import org.streams.commons.io.net.impl.RandomDistAddressSelector;
import org.streams.commons.metrics.CounterMetric;
import org.streams.commons.metrics.impl.MetricChannel;
import org.streams.commons.metrics.impl.MetricsAppService;
import org.streams.commons.zookeeper.ZGroup;
import org.streams.commons.zookeeper.ZLock;
import org.streams.commons.zookeeper.ZStore;
import org.streams.commons.zookeeper.ZStoreExpireCheckService;
import org.streams.coordination.cli.startup.service.impl.CollectorServerService;
import org.streams.coordination.cli.startup.service.impl.OrphanedFilesCheckService;

@Configuration
@Import(MetricsDI.class)
public class CollectorDI {

	private static final Logger LOG = Logger.getLogger(CollectorDI.class);

	@Inject
	BeanFactory beanFactory;

	@Bean
	public AppStartCommand startCollector() throws Exception {
		AppStartCommand agent = new AppStartCommand();
		agent.setAppLifeCycleManager(beanFactory
				.getBean(AppLifeCycleManager.class));
		return agent;
	}

	@Bean
	public AppLifeCycleManagerImpl appLifeCycleManager() throws Exception {

		List<? extends StartupCheck> preStartupCheckList = Arrays.asList(
				beanFactory.getBean(ConfigCheck.class),
				beanFactory.getBean(CodecCheck.class));

		List<? extends ApplicationService> serviceList = Arrays.asList(
				new RestletService((Component) beanFactory
						.getBean("restletPingComponent")), new RestletService(
						(Component) beanFactory.getBean("restletComponent")),
				beanFactory.getBean(CollectorServerService.class), beanFactory
						.getBean(GroupHeartbeatService.class), beanFactory
						.getBean(MetricsAppService.class), beanFactory
						.getBean(OrphanedFilesCheckService.class));

		// beanFactory
		// .getBean(ZStoreExpireCheckService.class));

		List<? extends StartupCheck> postStartupList = Arrays
				.asList(beanFactory.getBean(PingCheck.class));

		return new AppLifeCycleManagerImpl(preStartupCheckList, serviceList,
				postStartupList);
	}

	@Bean
	public OrphanedFilesCheckService OrphanedFilesCheckService() {
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		long frequency = configuration.getLong(
				CollectorProperties.WRITER.ORPHANED_LOG_CHECK_FREQUENCY
						.toString(),
				(Long) CollectorProperties.WRITER.ORPHANED_LOG_CHECK_FREQUENCY
						.getDefaultValue());

		return new OrphanedFilesCheckService(
				beanFactory.getBean(OrphanedFilesCheck.class), 10000L,
				frequency);
	}

	@Bean
	public OrphanedFilesCheck OrphanedFilesCheck() {
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		String baseDir = configuration.getString(
				CollectorProperties.WRITER.BASE_DIR.toString(),
				CollectorProperties.WRITER.BASE_DIR.getDefaultValue()
						.toString());

		long lowerMod = configuration.getLong(
				CollectorProperties.WRITER.ORPHANED_FILE_LOWER_MODE.toString(),
				(Long) CollectorProperties.WRITER.ORPHANED_FILE_LOWER_MODE
						.getDefaultValue());

		File file = new File(baseDir);

		return new OrphanedFilesCheckImpl(file,
				beanFactory.getBean(LogRolloverCheck.class),
				beanFactory.getBean(LogFileNameExtractor.class),
				beanFactory.getBean(LogRollover.class),
				beanFactory.getBean(FileOutputStreamPoolFactory.class).getPoolForKey("orphanedFiles"), lowerMod);

	}

	@Bean
	public GroupKeeper groupKeeper() throws KeeperException,
			InterruptedException, IOException {

		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		String[] hostsArr = configuration
				.getStringArray(CollectorProperties.WRITER.COORDINATION_HOST
						.toString());

		StringBuilder buff = new StringBuilder();
		int i = 0;
		for (String host : hostsArr) {
			if (i++ != 0)
				buff.append(',');

			buff.append(host);
		}

		String hosts = buff.toString();
		String group = configuration.getString(
				CollectorProperties.WRITER.COORDINATION_GROUP.toString(),
				CollectorProperties.WRITER.COORDINATION_GROUP.getDefaultValue()
						.toString());

               Long timeout = configuration.getLong(
				CollectorProperties.WRITER.ZOOTIMEOUT.toString(),
				(Long)CollectorProperties.WRITER.ZOOTIMEOUT.getDefaultValue()
						);

                if(timeout == null)
                   timeout = new Long(80000);

		return new ZGroup(group, hosts, timeout);

	}

	@Bean
	public GroupHeartbeatService groupHeartbeatService() throws BeansException,
			UnknownHostException {

		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		long initialDelay = configuration.getLong(
				CollectorProperties.WRITER.ZSTORE_TIMEOUT_DELAY.toString(),
				(Long) CollectorProperties.WRITER.ZSTORE_TIMEOUT_DELAY
						.getDefaultValue());

		long checkFrequency = configuration.getLong(
				CollectorProperties.WRITER.HEARTBEAT_FREQUENCY.toString(),
				(Long) CollectorProperties.WRITER.HEARTBEAT_FREQUENCY
						.getDefaultValue());

		int port = configuration.getInt(
				CollectorProperties.WRITER.COLLECTOR_PORT.toString(),
				(Integer) CollectorProperties.WRITER.COLLECTOR_PORT
						.getDefaultValue());

		GroupHeartbeatService service = new GroupHeartbeatService(
				beanFactory.getBean(GroupKeeper.class),
				Group.GroupStatus.Type.COLLECTOR,
				beanFactory.getBean(CollectorStatus.class), port, initialDelay,
				checkFrequency, 10000L);

		return service;
	}

	@Bean
	public ZStoreExpireCheckService zStoreExpireCheckService() {
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		long initialDelay = configuration.getLong(
				CollectorProperties.WRITER.ZSTORE_TIMEOUT_DELAY.toString(),
				(Long) CollectorProperties.WRITER.ZSTORE_TIMEOUT_DELAY
						.getDefaultValue());

		long checkFrequency = configuration.getLong(
				CollectorProperties.WRITER.ZSTORE_TIMEOUT_CHECK.toString(),
				(Long) CollectorProperties.WRITER.ZSTORE_TIMEOUT_CHECK
						.getDefaultValue());

		int dataTimeOut = configuration.getInt(
				CollectorProperties.WRITER.ZSTORE_DATA_TIMEOUT.toString(),
				(Integer) CollectorProperties.WRITER.ZSTORE_DATA_TIMEOUT
						.getDefaultValue());

		ZStoreExpireCheckService z = new ZStoreExpireCheckService();
		z.setInitialDelay(initialDelay);
		z.setCheckFrequency(checkFrequency);
		z.setDataTimeOut(dataTimeOut);

		return z;
	}

	@Bean
	public CollectorServer collectorServer() {

		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		int port = configuration.getInt(
				CollectorProperties.WRITER.COLLECTOR_PORT.toString(),
				(Integer) CollectorProperties.WRITER.COLLECTOR_PORT
						.getDefaultValue());

		CollectorServerImpl server = new CollectorServerImpl(port,
				beanFactory.getBean(LogWriterHandler.class), configuration,
				beanFactory.getBean(MetricChannel.class),
				beanFactory.getBean(IpFilterHandler.class));

		server.setReadTimeout(configuration.getLong(
				CollectorProperties.WRITER.COLLECTOR_CONNECTION_READ_TIMEOUT
						.toString(),
				(Long) CollectorProperties.WRITER.COLLECTOR_CONNECTION_READ_TIMEOUT
						.getDefaultValue()));
		server.setWriteTimeout(configuration.getLong(
				CollectorProperties.WRITER.COLLECTOR_CONNECTION_WRITE_TIMEOUT
						.toString(),
				(Long) CollectorProperties.WRITER.COLLECTOR_CONNECTION_WRITE_TIMEOUT
						.getDefaultValue()));

		return server;
	}

	@Bean
	public LogWriterHandler logWriterHandler() {
		org.apache.commons.configuration.Configuration conf = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		return new LogWriterHandler(beanFactory.getBean(Protocol.class), conf,
				new org.apache.hadoop.conf.Configuration(),
				beanFactory.getBean(LogFileWriter.class),
				beanFactory.getBean(CoordinationServiceClient.class),
				beanFactory.getBean(CollectorStatus.class),
				beanFactory.getBean("fileKilobytesWrittenMetric",
						CounterMetric.class),
				beanFactory.getBean(CompressionPoolFactory.class));

	}

	@Bean
	public CollectorServerService sollectorServerService() {
		return new CollectorServerService(
				beanFactory.getBean(CollectorServer.class));
	}

	@Bean
	public IpFilterHandler ipFilterHandler() {
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		IpFilterHandler filter = new IpFilterHandler();
		String[] hosts = configuration
				.getStringArray(CollectorProperties.WRITER.BLOCKED_IPS
						.toString());

		if (hosts != null)
			filter.getBlockedIps().addAll(Arrays.asList(hosts));

		return filter;
	}

	@Bean
	public CoordinationServiceClient coordinationServiceClient() {
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		String[] hostsArr = configuration
				.getStringArray(CollectorProperties.WRITER.COORDINATION_HOST
						.toString());

		StringBuilder buff = new StringBuilder();
		int i = 0;
		for (String host : hostsArr) {
			if (i++ != 0)
				buff.append(',');

			buff.append(host);
		}

		String hosts = buff.toString();
		String group = configuration.getString(
				CollectorProperties.WRITER.COORDINATION_GROUP.toString(),
				CollectorProperties.WRITER.COORDINATION_GROUP.getDefaultValue()
						.toString());

              Long timeout = configuration.getLong(
				CollectorProperties.WRITER.ZOOTIMEOUT.toString(),
				(Long)CollectorProperties.WRITER.ZOOTIMEOUT.getDefaultValue()
						);

                if(timeout == null)
                   timeout = new Long(80000);

		ZStoreExpireCheckService expireCheckService = beanFactory
				.getBean(ZStoreExpireCheckService.class);
		ZStore zstore = new ZStore("/coordination/" + group, hosts, timeout);

		expireCheckService.getStores().add(zstore);

		return new CoordinationServiceClientImpl(new ZLock(hosts, timeout),
				zstore);
	}

	/**
	 * Configure and start the CoordinationAddresses with the lock and unlock
	 * addresses
	 * 
	 * @return
	 */
	@Bean
	public CoordinationAddresses coordinationAddresses() {

		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		int lockport = configuration.getInt(
				CollectorProperties.WRITER.COORDINATION_LOCK_PORT.toString(),
				(Integer) CollectorProperties.WRITER.COORDINATION_LOCK_PORT
						.getDefaultValue());

		int unlockport = configuration.getInt(
				CollectorProperties.WRITER.COORDINATION_UNLOCK_PORT.toString(),
				(Integer) CollectorProperties.WRITER.COORDINATION_UNLOCK_PORT
						.getDefaultValue());

		String[] hostArr = configuration
				.getStringArray(CollectorProperties.WRITER.COORDINATION_HOST
						.toString());

		if (hostArr == null || hostArr.length < 1) {
			hostArr = new String[] { CollectorProperties.WRITER.COORDINATION_HOST
					.getDefaultValue().toString() };
		}

		InetSocketAddress[] lockAddresses = new InetSocketAddress[hostArr.length];
		for (int i = 0; i < hostArr.length; i++) {
			lockAddresses[i] = new InetSocketAddress(hostArr[i], lockport);
		}

		InetSocketAddress[] unLockAddresses = new InetSocketAddress[hostArr.length];
		for (int i = 0; i < hostArr.length; i++) {
			unLockAddresses[i] = new InetSocketAddress(hostArr[i], unlockport);
		}

		LOG.info("Using coordination addresses: " + Arrays.asList(hostArr));

		return new CoordinationAddresses(new RandomDistAddressSelector(
				lockAddresses), new RandomDistAddressSelector(unLockAddresses));
	}

	@Bean
	public LogFileNameExtractor logFileNameExtractor() throws Exception {

		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		String cls = configuration
				.getString(CollectorProperties.WRITER.LOG_NAME_EXTRACTOR
						.toString());

		LogFileNameExtractor nameExtractor = null;
		if (cls == null) {
			String keys = configuration.getString(
					CollectorProperties.WRITER.LOG_NAME_KEYS.toString(),
					CollectorProperties.WRITER.LOG_NAME_KEYS.getDefaultValue()
							.toString());

			String[] splits = keys.split(",");
			nameExtractor = new DateHourFileNameExtractor(splits);
		} else {
			nameExtractor = (LogFileNameExtractor) Thread.currentThread()
					.getContextClassLoader().loadClass(cls).newInstance();
		}

		return nameExtractor;
	}

	@Bean
	public PingOKResource pingOKResource() {
		return new PingOKResource();
	}

	/**
	 * Configures a restlet ping component
	 * 
	 * @return
	 */
	@Bean
	public Component restletPingComponent() {
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);
		Component component = new Component();

		component.getServers().add(
				org.restlet.data.Protocol.HTTP,
				configuration.getInt(CollectorProperties.WRITER.PING_PORT
						.toString(),
						(Integer) CollectorProperties.WRITER.PING_PORT
								.getDefaultValue()));
		component.getDefaultHost().attach(restletPingApplication());

		return component;
	}

	/**
	 * Configures a restlet component
	 * 
	 * @return
	 */
	@Bean
	public Component restletComponent() {
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);
		Component component = new Component();

		int port = configuration.getInt(
				CollectorProperties.WRITER.COLLECTOR_MON_PORT.toString(),
				(Integer) CollectorProperties.WRITER.COLLECTOR_MON_PORT
						.getDefaultValue());

		LOG.info("Using collector monitoring port: " + port);

		component.getServers().add(org.restlet.data.Protocol.HTTP, port);
		component.getDefaultHost().attach(restApplication());

		return component;
	}

	/**
	 * Returns a restlet application object with the correct router
	 * configuration and resources added.
	 * 
	 * @return
	 */
	@Bean
	public Application restApplication() {

		final Router router = new Router();
		attachFinder(router, "/collector/status",
				CollectorStatusResource.class, Template.MODE_STARTS_WITH);

		attachFinder(router, "/config", CollectorConfigResource.class,
				Template.MODE_STARTS_WITH);

		attachFinder(router, "/collector/shutdown", AppShutdownResource.class,
				Template.MODE_STARTS_WITH);

		attachFinder(router, "/collectors/status",
				CollectorsStatusResource.class, Template.MODE_STARTS_WITH);

		attachFinder(router, "/agents/status", AgentsStatusResource.class,
				Template.MODE_STARTS_WITH);

		Application app = new Application() {

			@Override
			public Restlet createInboundRoot() {
				return router;
			}

		};

		return app;
	}

	/**
	 * Returns a restlet application for the PingOKResource
	 * 
	 * @return
	 */
	@Bean
	public Application restletPingApplication() {

		final Router router = new Router();
		attachFinder(router, "/", PingOKResource.class,
				Template.MODE_STARTS_WITH);

		Application app = new Application() {

			@Override
			public Restlet createInboundRoot() {
				return router;
			}

		};

		return app;
	}

	@Bean
	public AppShutdownResource appShutdownResource() {
		return new AppShutdownResource(
				beanFactory.getBean(AppLifeCycleManager.class));
	}

	@Bean
	public CollectorConfigResource collectorConfigResource() {
		return new CollectorConfigResource(
				beanFactory
						.getBean(org.apache.commons.configuration.Configuration.class));
	}

	@Bean
	public CollectorStatusResource collectorStatusResource() {
		return new CollectorStatusResource(
				beanFactory.getBean(CollectorStatus.class));
	}

	@Bean
	public CollectorsStatusResource collectorsStatusResource() {
		return new CollectorsStatusResource(
				beanFactory.getBean(GroupKeeper.class));
	}

	@Bean
	public AgentsStatusResource agentsStatusResource() {
		return new AgentsStatusResource(beanFactory.getBean(GroupKeeper.class));
	}

	/**
	 * Helper method to attch the server resource to the router.
	 * 
	 * @param router
	 * @param pathTemplate
	 * @param resourceClass
	 * @param matchingMode
	 */
	private final void attachFinder(Router router, String pathTemplate,
			final Class<? extends ServerResource> resourceClass,
			int matchingMode) {

		Finder finder = new Finder() {

			@Override
			public ServerResource find(Request request, Response response) {
				return beanFactory.getBean(resourceClass);
			}

		};

		router.attach(pathTemplate, finder, matchingMode);
	}

}
