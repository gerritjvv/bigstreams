package org.streams.collector.di.impl;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.log4j.Logger;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.Restlet;
import org.restlet.resource.Finder;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;
import org.restlet.routing.Template;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.streams.collector.cli.startup.check.impl.CodecCheck;
import org.streams.collector.cli.startup.check.impl.ConfigCheck;
import org.streams.collector.cli.startup.check.impl.PingCheck;
import org.streams.collector.conf.CollectorProperties;
import org.streams.collector.mon.CollectorStatus;
import org.streams.collector.mon.impl.CollectorStatusResource;
import org.streams.collector.mon.impl.PingOKResource;
import org.streams.collector.server.CollectorServer;
import org.streams.collector.server.impl.CollectorServerImpl;
import org.streams.collector.server.impl.LogWriterHandler;
import org.streams.collector.write.LogFileNameExtractor;
import org.streams.collector.write.LogFileWriter;
import org.streams.collector.write.impl.DateHourFileNameExtractor;
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
import org.streams.commons.io.Protocol;
import org.streams.commons.metrics.CounterMetric;
import org.streams.commons.metrics.impl.MetricChannel;
import org.streams.commons.metrics.impl.MetricsAppService;
import org.streams.coordination.cli.startup.service.impl.CollectorServerService;

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
						.getBean(MetricsAppService.class));

		List<? extends StartupCheck> postStartupList = Arrays
				.asList(beanFactory.getBean(PingCheck.class));

		return new AppLifeCycleManagerImpl(preStartupCheckList, serviceList,
				postStartupList);
	}

	@Bean
	public CollectorServer collectorServer() {

		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		int port = configuration.getInt(
				CollectorProperties.WRITER.COLLECTOR_PORT.toString(),
				(Integer) CollectorProperties.WRITER.COLLECTOR_PORT
						.getDefaultValue());

		return new CollectorServerImpl(port,
				beanFactory.getBean(LogWriterHandler.class),
				beanFactory.getBean(CoordinationServiceClient.class),
				configuration, beanFactory.getBean(MetricChannel.class));
	}

	@Bean
	public LogWriterHandler logWriterHandler() {
		return new LogWriterHandler(
				beanFactory.getBean(Protocol.class),
				beanFactory
						.getBean(org.apache.commons.configuration.Configuration.class),
				new org.apache.hadoop.conf.Configuration(), beanFactory
						.getBean(LogFileWriter.class), beanFactory
						.getBean(CoordinationServiceClient.class), beanFactory
						.getBean(CollectorStatus.class), beanFactory.getBean(
						"fileKilobytesWrittenMetric", CounterMetric.class),
						beanFactory.getBean(CompressionPoolFactory.class));
	}

	@Bean
	public CollectorServerService sollectorServerService() {
		return new CollectorServerService(
				beanFactory.getBean(CollectorServer.class));
	}

	@Bean
	public CoordinationServiceClient coordinationServiceClient() {
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

		String hostname = configuration.getString(
				CollectorProperties.WRITER.COORDINATION_HOST.toString(),
				(String) CollectorProperties.WRITER.COORDINATION_HOST
						.getDefaultValue());

		return new CoordinationServiceClientImpl(new InetSocketAddress(
				hostname, lockport),
				new InetSocketAddress(hostname, unlockport));
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

		attachFinder(router, "/collector/shutdown", AppShutdownResource.class,
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
	public CollectorStatusResource collectorStatusResource() {
		return new CollectorStatusResource(
				beanFactory.getBean(CollectorStatus.class));
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
