package org.streams.coordination.cli.impl;

import java.io.OutputStream;
import java.io.PrintWriter;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.Configuration;
import org.restlet.data.MediaType;
import org.restlet.resource.ClientResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.streams.commons.cli.CommandLineProcessor;
import org.streams.coordination.CoordinationProperties;


/**
 * 
 * Implements the logic for stopping the coordination service.<br/>
 * 
 * 
 */
@Named("stopCoordination")
public class StopCoordinationCommand implements CommandLineProcessor {

	org.restlet.Client client;
	Configuration configuration;

	public StopCoordinationCommand(){}
	
	public StopCoordinationCommand(org.restlet.Client client,
			Configuration configuration) {
		this.client = client;
		this.configuration = configuration;

	}

	/**
	 * Counts the FileTrackingStatus instances
	 */
	@Override
	public void process(CommandLine cmdLine, OutputStream out) throws Exception {

		int clientPort = configuration.getInt(
				CoordinationProperties.PROP.COORDINATION_PORT.toString(),
				(Integer) CoordinationProperties.PROP.COORDINATION_PORT
						.getDefaultValue());

		ClientResource clientResource = new ClientResource("http://localhost:"
				+ clientPort + "/coordination/shutdown");

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
