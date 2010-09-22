package org.streams.agent.cli.impl;

import java.io.OutputStream;
import java.io.PrintWriter;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.restlet.data.MediaType;
import org.restlet.resource.ClientResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.streams.agent.conf.AgentProperties;
import org.streams.commons.cli.CommandLineProcessor;


/**
 * Implements the count command
 * 
 */
@Named("agentStatus")
public class AgentStatusCommand implements CommandLineProcessor {

	private static final Logger LOG = Logger
			.getLogger(AgentStatusCommand.class);

	org.restlet.Client client;
	Configuration configuration;

	public AgentStatusCommand() {
	}

	/**
	 * Counts the FileTrackingStatus instances
	 */
	@Override
	public void process(CommandLine cmdLine, OutputStream out) throws Exception {

		PrintWriter writer = new PrintWriter(out);
		try {
			if (client == null) {
				// if no client is injected print out shutdown
				writer.println("shutdown");
			} else {

				LOG.info("Connecting via rest");

				int clientPort = configuration.getInt(
						AgentProperties.MONITORING_PORT, 8040);

				LOG.info("Connecting client to " + clientPort);

				ClientResource clientResource = new ClientResource(
						"http://localhost:" + clientPort + "/agent/status");

				try {
					clientResource.get(MediaType.APPLICATION_JSON)
							.write(writer);
				} finally {
					clientResource.release();
				}

			}
		} finally {
			writer.close();
		}

	}

	@Autowired(required = false)
	public void setClient(org.restlet.Client client) {
		this.client = client;
	}

	@Inject
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

}
