package org.streams.agent.cli.impl;

import java.io.OutputStream;
import java.io.PrintWriter;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.Configuration;
import org.restlet.data.MediaType;
import org.restlet.resource.ClientResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.streams.agent.conf.AgentProperties;
import org.streams.commons.cli.CommandLineProcessor;


/**
 * 
 * Implements the logic for stopping the agent.<br/>
 * The logic for this is interesting as the agent will have been started in a
 * different process.<br/>
 * 
 */
@Named("stopAgent")
public class StopAgent implements CommandLineProcessor {

	org.restlet.Client client;
	Configuration configuration;

	public StopAgent(){
		
	}
	
	public StopAgent(org.restlet.Client client, Configuration configuration) {
		this.client = client;
		this.configuration = configuration;

	}

	/**
	 * Counts the FileTrackingStatus instances
	 */
	@Override
	public void process(CommandLine cmdLine, OutputStream out) throws Exception {

		int clientPort = configuration.getInt(AgentProperties.MONITORING_PORT,
				8040);

		ClientResource clientResource = new ClientResource("http://localhost:"
				+ clientPort + "/agent/shutdown");

		PrintWriter writer = new PrintWriter(out);
		try {

			try {
				clientResource.get(MediaType.APPLICATION_JSON).write(writer);
			} finally {
				clientResource.release();
			}

		} finally {
			writer.close();
		}

	}

	public org.restlet.Client getClient() {
		return client;
	}

	@Autowired(required=false)
	public void setClient(org.restlet.Client client) {
		this.client = client;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	@Inject
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

}
