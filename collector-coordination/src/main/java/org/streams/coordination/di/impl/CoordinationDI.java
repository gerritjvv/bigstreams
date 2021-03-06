package org.streams.coordination.di.impl;

import java.util.Arrays;
import java.util.List;

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
import org.streams.commons.app.AppLifeCycleManager;
import org.streams.commons.app.ApplicationService;
import org.streams.commons.app.StartupCheck;
import org.streams.commons.app.impl.AppLifeCycleManagerImpl;
import org.streams.commons.app.impl.AppShutdownResource;
import org.streams.commons.app.impl.RestletService;
import org.streams.commons.cli.AppStartCommand;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusKey;
import org.streams.commons.metrics.impl.MetricChannel;
import org.streams.commons.metrics.impl.MetricsAppService;
import org.streams.coordination.CoordinationProperties;
import org.streams.coordination.cli.startup.check.impl.ConfigCheck;
import org.streams.coordination.cli.startup.check.impl.HazelcastServiceCheck;
import org.streams.coordination.cli.startup.check.impl.PersistenceCheck;
import org.streams.coordination.cli.startup.service.impl.CoordinationServerService;
import org.streams.coordination.cli.startup.service.impl.HazelcastStartupService;
import org.streams.coordination.cli.startup.service.impl.StatusCleanoutService;
import org.streams.coordination.file.AgentContact;
import org.streams.coordination.file.CollectorFileTrackerMemory;
import org.streams.coordination.file.DistributedMapNames;
import org.streams.coordination.file.LogTypeContact;
import org.streams.coordination.file.history.FileTrackerHistoryItem;
import org.streams.coordination.file.history.FileTrackerHistoryMemory;
import org.streams.coordination.file.history.impl.hazelcast.FileTrackerHistoryMemoryImpl;
import org.streams.coordination.file.impl.hazelcast.FileTrackingStatusHazelcastMapStore;
import org.streams.coordination.file.impl.hazelcast.HazelcastFileTrackerStorage;
import org.streams.coordination.mon.CoordinationStatus;
import org.streams.coordination.mon.history.impl.CoordinationFileTrackingHistoryResource;
import org.streams.coordination.mon.history.impl.CoordinationHistoryAgentsLatestResource;
import org.streams.coordination.mon.history.impl.CoordinationHistoryCollectorsLatestResource;
import org.streams.coordination.mon.impl.CoordinationAgentCountResource;
import org.streams.coordination.mon.impl.CoordinationAgentNamesResource;
import org.streams.coordination.mon.impl.CoordinationClusterResource;
import org.streams.coordination.mon.impl.CoordinationConfigResource;
import org.streams.coordination.mon.impl.CoordinationFileTrackingCountResource;
import org.streams.coordination.mon.impl.CoordinationFileTrackingResource;
import org.streams.coordination.mon.impl.CoordinationLogTypeCountResource;
import org.streams.coordination.mon.impl.CoordinationLogTypesResource;
import org.streams.coordination.mon.impl.CoordinationStatusImpl;
import org.streams.coordination.mon.impl.CoordinationStatusResource;
import org.streams.coordination.mon.impl.FileStatusCleanoutManager;
import org.streams.coordination.service.CoordinationServer;
import org.streams.coordination.service.LockMemory;
import org.streams.coordination.service.impl.CoordinationLockHandler;
import org.streams.coordination.service.impl.CoordinationServerImpl;
import org.streams.coordination.service.impl.CoordinationUnLockHandler;
import org.streams.coordination.service.impl.HazelcastLockMemory;
import org.streams.coordination.service.impl.LockTimeoutCheckAppService;
import org.streams.coordination.service.impl.LockValue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

@Configuration
@Import(MetricsDI.class)
public class CoordinationDI {

	@Autowired
	BeanFactory beanFactory;

	@Bean
	public AppStartCommand startCoordination() throws Exception {
		AppStartCommand agent = new AppStartCommand();
		agent.setAppLifeCycleManager(beanFactory
				.getBean(AppLifeCycleManager.class));
		return agent;
	}

	@Bean
	public AppLifeCycleManagerImpl appLifeCycleManager() throws Exception {

		List<? extends StartupCheck> preStartupCheckList = Arrays.asList(
				beanFactory.getBean(ConfigCheck.class),
				beanFactory.getBean(PersistenceCheck.class));

		List<? extends ApplicationService> serviceList = Arrays.asList(
				beanFactory.getBean(HazelcastStartupService.class),
				beanFactory.getBean(CoordinationServerService.class),
				beanFactory.getBean(StatusCleanoutService.class),
				beanFactory.getBean(RestletService.class),
				beanFactory.getBean(MetricsAppService.class),
				beanFactory.getBean(LockTimeoutCheckAppService.class));

		List<? extends StartupCheck> postStartupCheckList = Arrays
				.asList(beanFactory.getBean(HazelcastServiceCheck.class));

		return new AppLifeCycleManagerImpl(preStartupCheckList, serviceList,
				postStartupCheckList);
	}

	@Bean
	public HazelcastStartupService hazelcastStartupService() {
		return new HazelcastStartupService(
				beanFactory
						.getBean(org.apache.commons.configuration.Configuration.class),
				beanFactory.getBean(FileTrackingStatusHazelcastMapStore.class));
	}

	@Bean
	public RestletService restletService() {
		return new RestletService(beanFactory.getBean(Component.class));
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
		component.getServers().add(
				org.restlet.data.Protocol.HTTP,
				configuration.getInt(
						CoordinationProperties.PROP.COORDINATION_PORT
								.toString(),
						(Integer) CoordinationProperties.PROP.COORDINATION_PORT
								.getDefaultValue()));
		component.getDefaultHost().attach(restletApplication());

		return component;
	}

	/**
	 * Returns a restlet application object with the correct router
	 * configuration and resources added.
	 * 
	 * @return
	 */
	@Bean
	public Application restletApplication() {

		// CoordinationStatusResource
		final Router router = new Router();

		attachFinder(router, "/config", CoordinationConfigResource.class,
				Template.MODE_EQUALS);

		attachFinder(router, "/cluster/status",
				CoordinationClusterResource.class, Template.MODE_EQUALS);

		attachFinder(router, "/coordination/status",
				CoordinationStatusResource.class, Template.MODE_EQUALS);

		attachFinder(router, "/coordination/shutdown",
				AppShutdownResource.class, Template.MODE_EQUALS);

		attachFinder(router, "/files/list",
				CoordinationFileTrackingResource.class,
				Template.MODE_STARTS_WITH);
		attachFinder(router, "/files/count",
				CoordinationFileTrackingCountResource.class,
				Template.MODE_STARTS_WITH);

		attachFinder(router, "/agents/list",
				CoordinationAgentNamesResource.class, Template.MODE_STARTS_WITH);
		attachFinder(router, "/agents/count",
				CoordinationAgentCountResource.class, Template.MODE_STARTS_WITH);

		attachFinder(router, "/logTypes/list",
				CoordinationLogTypesResource.class, Template.MODE_STARTS_WITH);
		attachFinder(router, "/logTypes/count",
				CoordinationLogTypeCountResource.class,
				Template.MODE_STARTS_WITH);

		attachFinder(router, "/agent/{agent}/history",
				CoordinationFileTrackingHistoryResource.class,
				Template.MODE_STARTS_WITH);

		attachFinder(router, "/agents/latest",
				CoordinationHistoryAgentsLatestResource.class,
				Template.MODE_STARTS_WITH);

		attachFinder(router, "/collectors/latest",
				CoordinationHistoryCollectorsLatestResource.class,
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
	public LockMemory lockMemory() {

		IMap<String, LockValue> locksMap = beanFactory.getBean(
				HazelcastInstance.class).getMap(
				DistributedMapNames.MAP.LOCK_MEMORY_LOCKS_MAP.toString());

		return new HazelcastLockMemory(locksMap);
	}

	@Bean
	public CoordinationServerService coordinationServerService() {
		return new CoordinationServerService(
				beanFactory.getBean(CoordinationServer.class));
	}

	@Bean
	public LockTimeoutCheckAppService lockTimeoutCheckAppService() {

		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		long lockTimeoutPeriod = configuration
				.getLong(
						CoordinationProperties.PROP.COORDINATION_LOCK_TIMEOUTCHECK_PERIOD
								.toString(),
						(Long) CoordinationProperties.PROP.COORDINATION_LOCK_TIMEOUTCHECK_PERIOD
								.getDefaultValue());

		long lockTimeout = configuration.getLong(
				CoordinationProperties.PROP.COORDINATION_LOCK_TIMEOUT
						.toString(),
				(Long) CoordinationProperties.PROP.COORDINATION_LOCK_TIMEOUT
						.getDefaultValue());

		return new LockTimeoutCheckAppService(lockTimeoutPeriod, lockTimeout,
				beanFactory.getBean(LockMemory.class));

	}

	@Bean
	public CoordinationServer coordinationServer() {
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		int lockPort = configuration.getInt(
				CoordinationProperties.PROP.COORDINATION_LOCK_PORT.toString(),
				(Integer) CoordinationProperties.PROP.COORDINATION_LOCK_PORT
						.getDefaultValue());

		int releaseLockPort = configuration
				.getInt(CoordinationProperties.PROP.COORDINATION_UNLOCK_PORT
						.toString(),
						(Integer) CoordinationProperties.PROP.COORDINATION_UNLOCK_PORT
								.getDefaultValue());

		return new CoordinationServerImpl(lockPort, releaseLockPort,
				beanFactory.getBean(CoordinationLockHandler.class), beanFactory.getBean(
						CoordinationUnLockHandler.class), beanFactory.getBean(
								MetricChannel.class));
	}

	@Bean
	@Lazy
	public CoordinationLockHandler coordinationLockHandler() {

		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);

		int port = configuration.getInt(
				CoordinationProperties.PROP.LOCK_HOLDER_PING_PORT.toString(),
				(Integer) CoordinationProperties.PROP.LOCK_HOLDER_PING_PORT
						.getDefaultValue());

		long lockTimeout = configuration.getLong(
				CoordinationProperties.PROP.COORDINATION_LOCK_TIMEOUT
						.toString(),
				(Long) CoordinationProperties.PROP.COORDINATION_LOCK_TIMEOUT
						.getDefaultValue());

		return new CoordinationLockHandler(
				beanFactory.getBean(CoordinationStatus.class),
				beanFactory.getBean(LockMemory.class),
				beanFactory.getBean(HazelcastFileTrackerStorage.class), port,
				lockTimeout,
				beanFactory.getBean(FileTrackerHistoryMemory.class));

	}

	@Bean
	public FileTrackerHistoryMemory fileTrackerHistoryMemory() {
		MultiMap<String, FileTrackerHistoryItem> historyMap = beanFactory
				.getBean(HazelcastInstance.class).getMultiMap(
						DistributedMapNames.MAP.FILE_TRACKER_HISTORY_MAP
								.toString());

		IMap<String, FileTrackerHistoryItem> latestMap = beanFactory.getBean(
				HazelcastInstance.class).getMap(
				DistributedMapNames.MAP.FILE_TRACKER_HISTORY_LATEST_MAP
						.toString());

		int threads = 10;

		return new FileTrackerHistoryMemoryImpl(historyMap, latestMap, threads);
	}

	@Bean
	@Lazy
	public CoordinationUnLockHandler coordinationUnLockHandler() {

		return new CoordinationUnLockHandler(
				beanFactory.getBean(CoordinationStatus.class),
				beanFactory.getBean(LockMemory.class),
				beanFactory.getBean(HazelcastFileTrackerStorage.class));

	}

	@Bean
	public HazelcastFileTrackerStorage hazelcastFileTrackerStorage() {
		HazelcastInstance hazelcastInstance = beanFactory
				.getBean(HazelcastInstance.class);

		IMap<FileTrackingStatusKey, FileTrackingStatus> fileTrackerMemory = hazelcastInstance
				.getMap(DistributedMapNames.MAP.FILE_TRACKER_MAP.toString());

		IMap<String, AgentContact> agentSet = hazelcastInstance
				.getMap(DistributedMapNames.MAP.AGENT_NAMES.toString());

		IMap<String, LogTypeContact> logTypeSet = hazelcastInstance
				.getMap(DistributedMapNames.MAP.LOG_TYPES.toString());

		return new HazelcastFileTrackerStorage(fileTrackerMemory, logTypeSet,
				agentSet);
	}

	@Bean
	public FileTrackingStatusHazelcastMapStore fileTrackingStatusHazelcastMapStore() {
		return new FileTrackingStatusHazelcastMapStore(beanFactory.getBean(
				"collectorFileTrackerMemory", CollectorFileTrackerMemory.class));
	}

	@Bean
	public synchronized HazelcastInstance hazelcastInstance() throws Exception {
		HazelcastStartupService startupService = beanFactory
				.getBean(HazelcastStartupService.class);

		startupService.start();

		return startupService.getHazelcastInstance();

	}

	// ------------- FileTrackerStatus History
	@Bean
	public CoordinationFileTrackingHistoryResource coordinationFileTrackingHistoryResource() {
		return new CoordinationFileTrackingHistoryResource(
				beanFactory.getBean(FileTrackerHistoryMemory.class));
	}

	@Bean
	public CoordinationHistoryAgentsLatestResource coordinationHistoryAgentsLatestResource() {
		return new CoordinationHistoryAgentsLatestResource(
				beanFactory.getBean(FileTrackerHistoryMemory.class));
	}

	@Bean
	public CoordinationHistoryCollectorsLatestResource coordinationHistoryCollectorsLatestResource() {
		return new CoordinationHistoryCollectorsLatestResource(
				beanFactory.getBean(FileTrackerHistoryMemory.class));
	}

	// ---------------------------------------

	@Bean
	public CoordinationFileTrackingCountResource coordinationFileTrackingCountResource() {
		return new CoordinationFileTrackingCountResource(
				beanFactory.getBean(HazelcastFileTrackerStorage.class));
	}

	@Bean
	public CoordinationFileTrackingResource coordinationFileTrackingResource() {
		return new CoordinationFileTrackingResource(
				beanFactory.getBean(HazelcastFileTrackerStorage.class));
	}

	@Bean
	public CoordinationAgentNamesResource coordinationAgentNamesResource() {
		return new CoordinationAgentNamesResource(
				beanFactory.getBean(HazelcastFileTrackerStorage.class));
	}

	@Bean
	public CoordinationAgentCountResource coordinationAgentCountResource() {
		return new CoordinationAgentCountResource(
				beanFactory.getBean(HazelcastFileTrackerStorage.class));
	}

	@Bean
	public CoordinationConfigResource coordinationConfigResource() {
		return new CoordinationConfigResource(
				beanFactory
						.getBean(org.apache.commons.configuration.Configuration.class));
	}

	@Bean
	public CoordinationClusterResource coordinationClusterResource() {
		return new CoordinationClusterResource(
				beanFactory.getBean(HazelcastStartupService.class));
	}

	@Bean
	public CoordinationStatusResource coordinationStatusResource() {
		return new CoordinationStatusResource(
				beanFactory.getBean(CoordinationStatus.class));
	}

	@Bean
	public CoordinationStatus coordinationStatus() {
		return new CoordinationStatusImpl();
	}

	@Bean
	public CoordinationLogTypesResource coordinationLogTypesResource() {
		return new CoordinationLogTypesResource(
				beanFactory.getBean(HazelcastFileTrackerStorage.class));
	}

	@Bean
	public CoordinationLogTypeCountResource coordinationLogTypeCountResource() {
		return new CoordinationLogTypeCountResource(
				beanFactory.getBean(HazelcastFileTrackerStorage.class));
	}

	@Bean
	public AppShutdownResource appShutdownResource() {
		return new AppShutdownResource(
				beanFactory.getBean(AppLifeCycleManager.class));
	}

	@Bean
	public StatusCleanoutService statusCleanoutService() throws Exception {
		// default once day
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);
		FileStatusCleanoutManager fileStatusCleanoutManager = beanFactory
				.getBean(FileStatusCleanoutManager.class);

		int statusCleanoutInterval = configuration
				.getInt(CoordinationProperties.PROP.STATUS_CLEANOUT_INTERVAL
						.toString(),
						(Integer) CoordinationProperties.PROP.STATUS_CLEANOUT_INTERVAL
								.getDefaultValue());

		StatusCleanoutService service = new StatusCleanoutService(
				fileStatusCleanoutManager, 10, statusCleanoutInterval);

		return service;
	}

	@Bean
	public FileStatusCleanoutManager fileStatusCleanoutManager()
			throws Exception {
		org.apache.commons.configuration.Configuration configuration = beanFactory
				.getBean(org.apache.commons.configuration.Configuration.class);
		CollectorFileTrackerMemory fileTrackerMemory = beanFactory
				.getBean(HazelcastFileTrackerStorage.class);

		long historyTimeLimit = configuration.getLong(
				CoordinationProperties.PROP.STATUS_HISTORY_LIMIT.toString(),
				(Long) CoordinationProperties.PROP.STATUS_HISTORY_LIMIT
						.getDefaultValue());

		FileStatusCleanoutManager fileStatusCleanoutManager = new FileStatusCleanoutManager(
				fileTrackerMemory, historyTimeLimit);

		return fileStatusCleanoutManager;
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
