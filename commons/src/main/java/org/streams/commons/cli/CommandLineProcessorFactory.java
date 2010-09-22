package org.streams.commons.cli;

public interface CommandLineProcessorFactory {

	enum PROFILE {
		DB, REST_CLIENT, AGENT, COLLECTOR, COORDINATION, CLI
	}

	
	/**
	 * Creates a CommandLineProcessor identified by the name with its related Profiles
	 * @param name
	 * @param profiles
	 * @return
	 */
	CommandLineProcessor create(String name, PROFILE... profiles);
	
}
