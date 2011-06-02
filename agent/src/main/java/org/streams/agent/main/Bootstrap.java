package org.streams.agent.main;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.streams.agent.cli.impl.AgentCommandLineParser;
import org.streams.agent.di.impl.AgentCommonDI;
import org.streams.agent.di.impl.AgentDI;
import org.streams.agent.di.impl.ConfigurationDI;
import org.streams.agent.di.impl.DBDI;
import org.streams.agent.di.impl.RestClientDI;
import org.streams.commons.cli.CommandLineParser;
import org.streams.commons.cli.CommandLineProcessor;
import org.streams.commons.cli.CommandLineProcessorFactory;


/**
 * 
 * The application has several different DI profiles and needs.<br/>
 * The needs are specified by each command via the AgentCommandLineParser and
 * the CommandProcessorFactory.<br/>
 * <p/>
 * 
 * The bootstrap is a DI configuration that defines the most basic DI
 * configuration.
 * <p/>
 * Basic profiles are:<br/>
 * <ul>
 * <li>ConfigurationDI</li>
 * <li>CommonDI</li>
 * </ul>
 */
public class Bootstrap {

	private static final Logger LOG = Logger.getLogger(Bootstrap.class);

	AnnotationConfigApplicationContext appContext;
	AtomicBoolean profilesLoaded = new AtomicBoolean(false);

	public Bootstrap() {
		appContext = new AnnotationConfigApplicationContext();
	}

	public CommandLineParser agentCommandLineParser() {
		return agentCommandLineParser(null);
	}

	public void printBeans() {

		for (String name : appContext.getBeanDefinitionNames()) {
			System.out.println(name);
		}
	}

	public void close(){
		appContext.close();
		appContext.destroy();
	}
	
	/**
	 * Loads a bean from the current DI context
	 * 
	 * @param <T>
	 * @param cls
	 * @return
	 */
	public <T> T getBean(Class<T> cls) {
		return appContext.getBean(cls);
	}

	/**
	 * Loads a bean from the current DI context
	 * 
	 * @param name
	 * @return
	 */
	public Object getBean(String name) {
		return appContext.getBean(name);
	}

	/**
	 * Returns an AgentCommandLineParser with a wrapping
	 * CommandLineProcessorFactory that will load the correct DI profiles.<br/>
	 * If the factory parameter is not null it would be used by the wrapping
	 * CommandLineProcessorFactory to create the CommandLineProcessor.
	 * 
	 * @param factory
	 * @return
	 */
	public CommandLineParser agentCommandLineParser(
			final CommandLineProcessorFactory factory) {

		// we inject a CommandLineProcessorFactory that will automatically load
		// the correct DI profiles
		// for each profile sent in its create method.
		// note that due to the way DI works this method can only be called once
		// ever in the Bootstrap instance

		CommandLineProcessorFactory profileFactory = new CommandLineProcessorFactory() {

			@Override
			public CommandLineProcessor create(String name, PROFILE... profiles) {

				loadProfiles(profiles);

				// allow for custom user injection of the factory
				// this is usefull for testing where we want the correct profile
				// to be loaded
				// but don't want the
				return (factory != null) ? factory.create(name, profiles)
						: getCommandLineProcessor(name);

			}
		};

		CommandLineParser parser = new AgentCommandLineParser(
				profileFactory);

		return parser;
	}

	/**
	 * Loads the list of profiles. Note that this method can only be called
	 * once, consecutive calls will be ignored.
	 * 
	 * @param profiles
	 */
	public void loadProfiles(CommandLineProcessorFactory.PROFILE... profiles) {

		if (profilesLoaded.compareAndSet(false, true)) {
			LOG.info("Loading DI profiles " + Arrays.toString(profiles));

			Set<Class<?>> DIClasses = new LinkedHashSet<Class<?>>();
			DIClasses.add(ConfigurationDI.class);

			Set<String> DIPackages = new LinkedHashSet<String>();

			for (CommandLineProcessorFactory.PROFILE profile : profiles) {
				switch (profile) {
				case DB:
					DIClasses.add(DBDI.class);
					break;
				case REST_CLIENT:
					DIClasses.add(RestClientDI.class);
					break;
				case CLI:
					DIPackages
							.add("org.streams.agent.cli.impl");
					break;
				case AGENT:
					DIPackages
					.add("org.streams.agent.mon.impl");
					DIClasses.add(AgentCommonDI.class);
					DIClasses.add(AgentDI.class);
					break;
				}
			}

			appContext.register(DIClasses.toArray(new Class<?>[] {}));
			appContext.scan(DIPackages.toArray(new String[] {}));
			appContext.refresh();
		} else {
			LOG.warn("The DI profiles have been loaded already");
		}
	}

	/**
	 * Returns the CommanLineProcessor bean with name == name
	 * 
	 * @param name
	 * @return
	 */
	private CommandLineProcessor getCommandLineProcessor(String name) {
		return (CommandLineProcessor) appContext.getBean(name);
	}

}
