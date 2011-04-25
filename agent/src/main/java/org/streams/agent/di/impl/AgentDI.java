package org.streams.agent.di.impl;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.inject.Inject;
import javax.persistence.EntityManagerFactory;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.Timer;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.streams.agent.agentcli.startup.check.impl.CodecCheck;
import org.streams.agent.agentcli.startup.check.impl.ConfigCheck;
import org.streams.agent.agentcli.startup.check.impl.FileTrackingStatusStartupCheck;
import org.streams.agent.agentcli.startup.check.impl.PersistenceCheck;
import org.streams.agent.agentcli.startup.service.impl.DirectoryPollingService;
import org.streams.agent.agentcli.startup.service.impl.StatusCleanoutService;
import org.streams.agent.conf.AgentConfiguration;
import org.streams.agent.conf.AgentProperties;
import org.streams.agent.conf.LogDirConf;
import org.streams.agent.file.DirectoryWatcher;
import org.streams.agent.file.DirectoryWatcherFactory;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.impl.ThreadedDirectoryWatcher;
import org.streams.agent.mon.impl.AgentShutdownResource;
import org.streams.agent.mon.impl.AgentStatusResource;
import org.streams.agent.mon.impl.FileStatusCleanoutManager;
import org.streams.agent.mon.impl.FileTrackingStatusCountResource;
import org.streams.agent.mon.impl.FileTrackingStatusPathResource;
import org.streams.agent.mon.impl.FileTrackingStatusResource;
import org.streams.agent.mon.status.AgentStatus;
import org.streams.agent.send.ClientConnectionFactory;
import org.streams.agent.send.ClientResourceFactory;
import org.streams.agent.send.FileSendTask;
import org.streams.agent.send.FileStreamer;
import org.streams.agent.send.FilesToSendQueue;
import org.streams.agent.send.ThreadResourceService;
import org.streams.agent.send.impl.ClientConnectionFactoryImpl;
import org.streams.agent.send.impl.ClientResourceFactoryImpl;
import org.streams.agent.send.impl.FileLineStreamerImpl;
import org.streams.agent.send.impl.FileSendTaskImpl;
import org.streams.agent.send.impl.FilesSendService;
import org.streams.agent.send.impl.FilesToSendQueueImpl;
import org.streams.agent.send.impl.ThreadResourceServiceImpl;
import org.streams.commons.app.AppLifeCycleManager;
import org.streams.commons.app.ApplicationService;
import org.streams.commons.app.StartupCheck;
import org.streams.commons.app.impl.AppLifeCycleManagerImpl;
import org.streams.commons.app.impl.RestletService;
import org.streams.commons.cli.AppStartCommand;
import org.streams.commons.compression.CompressionPoolFactory;
import org.streams.commons.io.Protocol;
import org.streams.commons.metrics.CounterMetric;
import org.streams.commons.metrics.impl.MetricsAppService;

/**
 * Injects the AppLifeCycleManager and all of its dependencies
 * 
 */
@Configuration
@Import(MetricsDI.class)
public class AgentDI {

	@Autowired(required = true)
	BeanFactory beanFactory;

	@Bean
	public AppStartCommand startAgent() throws Exception {
		AppStartCommand agent = new AppStartCommand();
		agent.setAppLifeCycleManager(beanFactory
				.getBean(AppLifeCycleManager.class));
		return agent;
	}

	@Bean
	public CodecCheck codecCheck() throws SecurityException,
			InstantiationException, IllegalAccessException,
			ClassNotFoundException, NoSuchFieldException {
		return new CodecCheck(beanFactory.getBean(CompressionCodec.class));
	}

	@Bean
	public ConfigCheck configCheck() throws IOException {
		return new ConfigCheck(beanFactory.getBean(LogDirConf.class),
				beanFactory.getBean(AgentConfiguration.class));
	}

	@Bean
	@Lazy
	public PersistenceCheck persistenceCheck() {
		return new PersistenceCheck(
				beanFactory.getBean(EntityManagerFactory.class));
	}

	@Bean
	@Lazy
	public FileTrackingStatusStartupCheck fileTrackingStatusStartupCheck() {
		return new FileTrackingStatusStartupCheck(
				beanFactory.getBean(FileTrackerMemory.class));
	}

	@Bean
	@Lazy
	public DirectoryPollingService directoryPollingService()
			throws IOException, Exception {
		return new DirectoryPollingService(
				beanFactory.getBean(DirectoryWatcherFactory.class),
				beanFactory.getBean(LogDirConf.class));
	}

	@Bean
	@Lazy
	public AppLifeCycleManagerImpl appLifeCycleManager() throws Exception {

		List<StartupCheck> preStartupCheckList = Arrays.asList(configCheck(),
				codecCheck(), persistenceCheck(),
				fileTrackingStatusStartupCheck());

		List<ApplicationService> serviceList = Arrays.asList(
				beanFactory.getBean(ThreadResourceService.class),
				beanFactory.getBean(DirectoryPollingService.class),
				beanFactory.getBean(RestletService.class),
				beanFactory.getBean(FilesSendService.class),
				beanFactory.getBean(MetricsAppService.class));

		return new AppLifeCycleManagerImpl(preStartupCheckList, serviceList,
				null);
	}

	@Bean
	@Lazy
	public StatusCleanoutService statusCleanoutService() throws Exception {
		// default once day
		AgentConfiguration conf = beanFactory.getBean(AgentConfiguration.class);
		FileStatusCleanoutManager fileStatusCleanoutManager = beanFactory
				.getBean(FileStatusCleanoutManager.class);

		int statusCleanoutInterval = conf.getStatusCleanoutInterval();

		StatusCleanoutService service = new StatusCleanoutService(
				fileStatusCleanoutManager, 60000, statusCleanoutInterval);

		return service;
	}

	@Bean
	public FilesSendService filesSendService() throws Exception {
		AgentConfiguration conf = beanFactory.getBean(AgentConfiguration.class);

		int clientThreadCount = conf.getClientThreadCount();

		return new FilesSendService(
				beanFactory.getBean(ClientResourceFactory.class),
				beanFactory.getBean(FileSendTask.class), clientThreadCount,
				beanFactory.getBean(AgentStatus.class),
				beanFactory.getBean(FileTrackerMemory.class),
				beanFactory.getBean(FilesToSendQueue.class));
	}

	@Bean
	public RestletService restletService() {
		return new RestletService(restletComponent());
	}

	/**
	 * Configures a restlet component
	 * 
	 * @return
	 */
	@Bean
	@Inject
	public Component restletComponent() {
		AgentConfiguration conf = beanFactory.getBean(AgentConfiguration.class);
		
		Component component = new Component();
		component.getServers().add(org.restlet.data.Protocol.HTTP,
				conf.getMonitoringPort());
		component.getDefaultHost().attach(restletApplication());

		return component;
	}

	/**
	 * Returns a restlet application object with the correct router
	 * configuration and resources added. Adds the
	 * FileTrackingStatusCountResource to files
	 * 
	 * @return
	 */
	@Bean
	public Application restletApplication() {

		Finder finderAgentShutdown = new Finder() {

			@Override
			public ServerResource find(Request request, Response response) {
				try {
					return new AgentShutdownResource(
							beanFactory.getBean(AppLifeCycleManager.class));
				} catch (Exception e) {
					RuntimeException rte = new RuntimeException();
					rte.setStackTrace(e.getStackTrace());
					throw rte;
				}
			}

		};

		Finder finderAgentStatus = new Finder() {

			@Override
			public ServerResource find(Request request, Response response) {
				return beanFactory.getBean(AgentStatusResource.class);
			}

		};

		Finder finderStatus = new Finder() {

			@Override
			public ServerResource find(Request request, Response response) {
				return fileTrackingStatusResource();
			}

		};
		Finder finderStatusCount = new Finder() {

			@Override
			public ServerResource find(Request request, Response response) {
				return fileTrackingStatusCountResource();
			}

		};
		// FileTrackingStatusPathResource
		Finder finderStatusPath = new Finder() {

			@Override
			public ServerResource find(Request request, Response response) {
				return fileTrackingStatusPathResource();
			}

		};
		final Router router = new Router();
		router.attach("/files/list/{status}", finderStatus);
		router.attach("/files/list", finderStatus);
		router.attach("/files/list/", finderStatus);
		router.attach("/agent/status", finderAgentStatus,
				Template.MODE_STARTS_WITH);
		router.attach("/files/count", finderStatusCount);
		router.attach("/files/count/{status}", finderStatusCount);
		// the match mode for this resource must be starts with because we
		// expect the file name with forward slashes
		// after the /files/status/ part.
		router.attach("/files/status", finderStatusPath,
				Template.MODE_STARTS_WITH);
		router.attach("/agent/shutdown", finderAgentShutdown);

		Application app = new Application() {

			@Override
			public Restlet createInboundRoot() {
				return router;
			}

		};

		return app;
	}

	@Bean
	@Lazy
	public AgentStatusResource agentStatusResource() {
		AgentStatusResource resource = new AgentStatusResource();
		resource.setStatus(beanFactory.getBean(AgentStatus.class));

		return resource;
	}

	@Bean
	@Lazy
	public FileStatusCleanoutManager fileStatusCleanoutManager()
			throws Exception {
		
		AgentConfiguration conf = beanFactory.getBean(AgentConfiguration.class);
		
		FileTrackerMemory fileTrackerMemory = beanFactory
				.getBean(FileTrackerMemory.class);

		long historyTimeLimit = conf.getStatusHistoryLimit();
		FileStatusCleanoutManager fileStatusCleanoutManager = new FileStatusCleanoutManager(
				fileTrackerMemory, historyTimeLimit);

		return fileStatusCleanoutManager;
	}

	/**
	 * Creates an anonymous instance of DirectoryWatcherFactory that would
	 * return an instance of ThreadedDirectoryWatcher An internal Map is used to
	 * make sure only one DirectoryWatcher is every instantiated for a
	 * directory.<br/>
	 * The DI will maintain the singleton pattern so that multiple threads won't
	 * break this.
	 * 
	 * @return
	 * @throws Exception
	 */
	@Bean
	@Lazy
	public DirectoryWatcherFactory directoryWatcherFactory() throws Exception {

		final AgentConfiguration conf = beanFactory.getBean(AgentConfiguration.class);
		
		final FileTrackerMemory fileTrackerMemory = beanFactory
				.getBean(FileTrackerMemory.class);
		DirectoryWatcherFactory fact = new DirectoryWatcherFactory() {

			Map<File, ThreadedDirectoryWatcher> map = new HashMap<File, ThreadedDirectoryWatcher>();

			@Override
			public DirectoryWatcher createInstance(String logType,
					File directory) {

				ThreadedDirectoryWatcher watcher = map.get(directory);
				if (watcher == null) {
					int pollingInterval = conf.getPollingInterval();
					
					watcher = new ThreadedDirectoryWatcher(logType,
							pollingInterval, fileTrackerMemory);
					watcher.setDirectory(directory.getAbsolutePath());

					map.put(directory, watcher);
				}
				return watcher;
			}

		};

		return fact;
	}

	/**
	 * Creates a FileStreamer instance either from the configuration property
	 * send.filelinestreamer.class or if not provided by using the default
	 * implementation FileLineStreamerImpl.
	 * <p/>
	 * Accepts configurable property send.filelinestreamer.buffersize
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	@Bean
	@Lazy
	public FileStreamer fileStreamer() throws Exception {
		AgentConfiguration conf = beanFactory.getBean(AgentConfiguration.class);
		
		CompressionCodec codec = beanFactory.getBean(CompressionCodec.class);

		String fileStreamerClass = conf.getFileStreamerClass();
		FileStreamer fileStreamer = null;

		long bufferSize = conf.getConnectionBufferSize();

		if (fileStreamerClass == null) {
			fileStreamer = new FileLineStreamerImpl(codec,
					beanFactory.getBean(CompressionPoolFactory.class),
					bufferSize);
		} else {
			fileStreamer = (FileStreamer) Thread.currentThread()
					.getContextClassLoader().loadClass(fileStreamerClass)
					.newInstance();

			fileStreamer.setCodec(codec);
			fileStreamer.setBufferSize(bufferSize);
		}

		return fileStreamer;
	}

	@Bean
	public ThreadResourceService threadResourceService() {
		return new ThreadResourceServiceImpl();
	}

	/**
	 * ClientConnection instances are done per call to this method.<br/>
	 * A client connection instance is meant to be used once and then thrown
	 * away. For this reason a Factory pattern is used.
	 * <p/>
	 * Used by the ClientResourceFactory.
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	@Bean
	public ClientConnectionFactory clientConnectionFactory()
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		AgentConfiguration conf = beanFactory.getBean(AgentConfiguration.class);
		Protocol protocol = beanFactory.getBean(Protocol.class);

		// @TODO configuration
		// load timeout parameters
		long sendTimeout = conf.getConnectionSendTimeout();
		long connectionEstablishTimeout = conf.getConnectionEstablishTimeout();

		ThreadResourceService resourceService = beanFactory
				.getBean(ThreadResourceService.class);

		Timer timer = resourceService.getTimer();
		ExecutorService workerBossService = resourceService
				.getWorkerBossService();
		ExecutorService workerService = resourceService.getWorkerService();

		// create factory passing the connection class to the factory.
		// the factory class will take charge or creating the connection
		// instances
		ClientConnectionFactory ccFact = new ClientConnectionFactoryImpl(timer,
				new NioClientSocketChannelFactory(workerBossService,
						workerService), connectionEstablishTimeout,
				sendTimeout, protocol);

		return ccFact;
	}

	/**
	 * Used by the FileSendTaskImpl to retreive a ClientResource for sending
	 * file data.<br/>
	 * Uses teh CilentConnectionFactory and FileStreamer.
	 * 
	 * @return
	 */
	@Bean
	public ClientResourceFactory clientResourceFactory() {
		return new ClientResourceFactoryImpl(
				beanFactory.getBean(ClientConnectionFactory.class),
				beanFactory.getBean(FileStreamer.class));
	}

	/**
	 * Used by the FileSendWorkerImpl to send file data. This class uses the
	 * ClientResourceFactory.
	 * 
	 * @return
	 * @throws MalformedURLException
	 */
	@Bean
	public FileSendTask fileSendTask() throws MalformedURLException {

		AgentConfiguration conf = beanFactory.getBean(AgentConfiguration.class);
		String collector = conf.getCollectorAddress();

		if (collector == null) {
			throw new RuntimeException(
					"Please define a collector address using the property "
							+ AgentProperties.COLLECTOR);
		}

		// parse the collector url, and error is thrown by the URL class if the
		// collector url is not correctly formed
		URL url = new URL(collector);
		InetSocketAddress collectorAddress = new InetSocketAddress(
				url.getHost(), url.getPort());

		return new FileSendTaskImpl(
				beanFactory.getBean(ClientResourceFactory.class),
				collectorAddress, beanFactory.getBean(FileTrackerMemory.class),
				beanFactory.getBean("fileKilobytesReadMetric",
						CounterMetric.class));

	}

	@Bean
	public FilesToSendQueue filesToSendQueue() {
		FileTrackerMemory fileTrackerMemory = beanFactory
				.getBean(FileTrackerMemory.class);
		return new FilesToSendQueueImpl(fileTrackerMemory);
	}

	@Bean
	public FileTrackingStatusCountResource fileTrackingStatusCountResource() {
		FileTrackerMemory fileTrackerMemory = beanFactory
				.getBean(FileTrackerMemory.class);
		FileTrackingStatusCountResource resource = new FileTrackingStatusCountResource();
		resource.setMemory(fileTrackerMemory);
		return resource;
	}

	@Bean
	public FileTrackingStatusResource fileTrackingStatusResource() {
		FileTrackerMemory fileTrackerMemory = beanFactory
				.getBean(FileTrackerMemory.class);
		FileTrackingStatusResource resource = new FileTrackingStatusResource();
		resource.setMemory(fileTrackerMemory);
		return resource;
	}

	@Bean
	public FileTrackingStatusPathResource fileTrackingStatusPathResource() {
		FileTrackerMemory fileTrackerMemory = beanFactory
				.getBean(FileTrackerMemory.class);
		FileTrackingStatusPathResource resource = new FileTrackingStatusPathResource();
		resource.setMemory(fileTrackerMemory);
		return resource;
	}

}
