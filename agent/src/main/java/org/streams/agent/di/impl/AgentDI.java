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

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.persistence.EntityManagerFactory;

import org.apache.hadoop.io.compress.CompressionCodec;
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
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.streams.agent.agentcli.startup.check.impl.CodecCheck;
import org.streams.agent.agentcli.startup.check.impl.ConfigCheck;
import org.streams.agent.agentcli.startup.check.impl.FileTrackingStatusStartupCheck;
import org.streams.agent.agentcli.startup.check.impl.PersistenceCheck;
import org.streams.agent.agentcli.startup.service.impl.ClientSendService;
import org.streams.agent.agentcli.startup.service.impl.DirectoryPollingService;
import org.streams.agent.agentcli.startup.service.impl.StatusCleanoutService;
import org.streams.agent.conf.AgentProperties;
import org.streams.agent.conf.LogDirConf;
import org.streams.agent.file.DirectoryWatcher;
import org.streams.agent.file.DirectoryWatcherFactory;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.impl.ThreadedDirectoryWatcher;
import org.streams.agent.mon.AgentStatus;
import org.streams.agent.mon.impl.AgentShutdownResource;
import org.streams.agent.mon.impl.AgentStatusImpl;
import org.streams.agent.mon.impl.AgentStatusResource;
import org.streams.agent.mon.impl.FileStatusCleanoutManager;
import org.streams.agent.mon.impl.FileTrackingStatusCountResource;
import org.streams.agent.mon.impl.FileTrackingStatusPathResource;
import org.streams.agent.mon.impl.FileTrackingStatusResource;
import org.streams.agent.send.Client;
import org.streams.agent.send.ClientConnection;
import org.streams.agent.send.ClientConnectionFactory;
import org.streams.agent.send.ClientSendThread;
import org.streams.agent.send.ClientSendThreadFactory;
import org.streams.agent.send.FileStreamer;
import org.streams.agent.send.FilesToSendQueue;
import org.streams.agent.send.ThreadContext;
import org.streams.agent.send.impl.AbstractClientConnectionFactory;
import org.streams.agent.send.impl.ClientConnectionImpl;
import org.streams.agent.send.impl.ClientFileSendThreadImpl;
import org.streams.agent.send.impl.ClientImpl;
import org.streams.agent.send.impl.FileLineStreamerImpl;
import org.streams.agent.send.impl.FilesToSendQueueImpl;
import org.streams.commons.app.AppLifeCycleManager;
import org.streams.commons.app.ApplicationService;
import org.streams.commons.app.StartupCheck;
import org.streams.commons.app.impl.AppLifeCycleManagerImpl;
import org.streams.commons.app.impl.RestletService;
import org.streams.commons.cli.AppStartCommand;
import org.streams.commons.io.Protocol;


/**
 * Injects the AppLifeCycleManager and all of its dependencies
 * 
 */
@Configuration
public class AgentDI {

	@Autowired(required = true)
	BeanFactory beanFactory;

	@Bean
	@Singleton
	public AgentStatus agentStatus() {
		return new AgentStatusImpl();
	}

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
		return new ConfigCheck(
				beanFactory.getBean(LogDirConf.class),
				beanFactory
						.getBean(org.apache.commons.configuration.Configuration.class));
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
				beanFactory.getBean(DirectoryPollingService.class),
				beanFactory.getBean(RestletService.class),
				beanFactory.getBean(ClientSendService.class));

		return new AppLifeCycleManagerImpl(preStartupCheckList, serviceList,
				null);
	}

	@Bean
	@Lazy
	public StatusCleanoutService statusCleanoutService() throws Exception {
		// default once day
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);
		FileStatusCleanoutManager fileStatusCleanoutManager = beanFactory
				.getBean(FileStatusCleanoutManager.class);

		int statusCleanoutInterval = configuration.getInt(
				AgentProperties.STATUS_CLEANOUT_INTERVAL, 86400);
		StatusCleanoutService service = new StatusCleanoutService(
				fileStatusCleanoutManager, 10, statusCleanoutInterval);

		return service;
	}

	@Bean
	@Lazy
	public ClientSendService clientSendService() throws Exception {
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);
		ClientSendThreadFactory clientSendThreadFactory = beanFactory
				.getBean(ClientSendThreadFactory.class);

		int clientThreadCount = configuration.getInt(
				AgentProperties.CLIENT_THREAD_COUNT, 5);

		ClientSendService service = new ClientSendService(clientThreadCount,
				clientSendThreadFactory);
		return service;
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
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);
		Component component = new Component();
		component.getServers().add(org.restlet.data.Protocol.HTTP,
				configuration.getInt(AgentProperties.MONITORING_PORT, 8040));
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
				return new AgentStatusResource(
						beanFactory.getBean(AgentStatus.class));
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
		router.attach("/agent/status", finderAgentStatus);
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
	public FileStatusCleanoutManager fileStatusCleanoutManager()
			throws Exception {
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);
		FileTrackerMemory fileTrackerMemory = beanFactory
				.getBean(FileTrackerMemory.class);

		long historyTimeLimit = configuration.getLong(
				AgentProperties.STATUS_HISTORY_LIMIT, Long.MAX_VALUE);
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
		final org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);
		final FileTrackerMemory fileTrackerMemory = beanFactory
				.getBean(FileTrackerMemory.class);
		DirectoryWatcherFactory fact = new DirectoryWatcherFactory() {

			Map<File, ThreadedDirectoryWatcher> map = new HashMap<File, ThreadedDirectoryWatcher>();

			@Override
			public DirectoryWatcher createInstance(String logType,
					File directory) {

				ThreadedDirectoryWatcher watcher = map.get(directory);
				if (watcher == null) {
					// get polling interval, default is 10 seconds
					int pollingInterval = configuration.getInt(
							AgentProperties.DIRECTORY_WATCH_POLL_INTERVAL, 10);
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
		org.apache.commons.configuration.Configuration conf = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);
		CompressionCodec codec = beanFactory.getBean(CompressionCodec.class);

		String fileStreamerClass = conf.getString(
				AgentProperties.FILE_STREAMER_CLASS, null);
		FileStreamer fileStreamer = null;

		long bufferSize = conf.getLong(
				AgentProperties.FILE_STREAMER_BUFFERSIZE, 1024 * 100);

		if (fileStreamerClass == null) {
			fileStreamer = new FileLineStreamerImpl(codec, bufferSize);
		} else {
			fileStreamer = (FileStreamer) Thread.currentThread()
					.getContextClassLoader().loadClass(fileStreamerClass)
					.newInstance();

			fileStreamer.setCodec(codec);
			fileStreamer.setBufferSize(bufferSize);
		}

		return fileStreamer;
	}

	/**
	 * ClientConnection instances are done per call to this method.<br/>
	 * A client connection instance is meant to be used once and then thrown
	 * away. For this reason a Factory pattern is used.
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

		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);
		Protocol protocol = beanFactory.getBean(Protocol.class);

		// find ClientConnection class either from configuration or from the
		// default class ClientConnectionImpl

		// load timeout parameters
		long sendTimeout = configuration.getLong(
				AgentProperties.CLIENTCONNECTION_SEND_TIMEOUT, 10000L);
		long connectionEstablishTimeout = configuration.getLong(
				AgentProperties.CLIENTCONNECTION_ESTABLISH_TIMEOUT, 20000L);

		// create factory passing the connection class to the factory.
		// the factory class will take charge or creating the connection
		// instances
		AbstractClientConnectionFactory fact = new AbstractClientConnectionFactory() {
			protected ClientConnection createConnection() {
				return new ClientConnectionImpl();
			}
		};

		fact.setConnectEstablishTimeout(connectionEstablishTimeout);
		fact.setSendTimeOut(sendTimeout);

		fact.setProtocol(protocol);

		return fact;
	}

	@SuppressWarnings("unchecked")
	@Bean
	@Scope("prototype")
	@Lazy
	public Client client() throws Exception {
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);
		String className = configuration
				.getString(AgentProperties.CLIENT_CLASS);

		Class<? extends Client> clientClass = null;

		if (className == null) {
			clientClass = ClientImpl.class;
		} else {
			clientClass = (Class<? extends Client>) Thread.currentThread()
					.getContextClassLoader().loadClass(className);
		}

		Client client = clientClass.newInstance();
		client.setClientConnectionFactory(clientConnectionFactory());
		client.setFileStreamer(fileStreamer());

		return client;
	}

	/**
	 * Creates an ExecutorService with as many client send threads as specified
	 * by the CLIENT_THREAD_COUNT property.
	 * 
	 * @return
	 * @throws MalformedURLException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	@Bean
	@Lazy
	public ClientSendThreadFactory clientSendThreadFactory()
			throws MalformedURLException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {

		return new ClientSendThreadFactory() {
			@Override
			public ClientSendThread get() {
				try {
					return clientFileSendThread();
				} catch (Exception t) {
					RuntimeException rte = new RuntimeException(t.toString(), t);
					rte.setStackTrace(t.getStackTrace());
					throw rte;
				}
			}
		};

	}

	/**
	 * Returns an instance of the ClientFileSendThread
	 * 
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws MalformedURLException
	 */
	@Bean
	@Scope("prototype")
	@Lazy
	public ClientSendThread clientFileSendThread() throws Exception {
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);
		FileTrackerMemory fileTrackerMemory = beanFactory
				.getBean(FileTrackerMemory.class);

		// get and parse the collector address
		org.apache.commons.configuration.Configuration conf = configuration;
		String collector = conf.getString(AgentProperties.COLLECTOR);

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

		// get wait and retry parameters
		long waitIfEmpty = conf.getLong(AgentProperties.THREAD_WAIT_IFEMPTY,
				60000L);
		int retries = conf.getInt(AgentProperties.THREAD_RETRIES, 3);

		AgentStatus agentStatus = beanFactory.getBean(AgentStatus.class);
		ThreadContext threadContext = new ThreadContext(fileTrackerMemory,
				filesToSendQueue(), client(), collectorAddress, agentStatus,
				waitIfEmpty, retries);

		ClientFileSendThreadImpl thread = new ClientFileSendThreadImpl(
				threadContext);

		agentStatus.incCounter("CLIENT_SEND_THREAD", 1);
		return thread;
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
