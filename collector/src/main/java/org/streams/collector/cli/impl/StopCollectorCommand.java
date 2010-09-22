package org.streams.collector.cli.impl;

import java.io.OutputStream;
import java.io.PrintWriter;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.Configuration;
import org.restlet.data.MediaType;
import org.restlet.resource.ClientResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.streams.collector.conf.CollectorProperties;
import org.streams.commons.cli.CommandLineProcessor;


/**
 * 
 * Implements the logic for stopping the collector service.<br/>
 * 
 * 
 */
@Named("stopCollector")
public class StopCollectorCommand implements CommandLineProcessor {

	org.restlet.Client client;
	Configuration configuration;

	public StopCollectorCommand(){}
	
	public StopCollectorCommand(org.restlet.Client client,
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
				CollectorProperties.WRITER.COLLECTOR_MON_PORT.toString(),
				(Integer) CollectorProperties.WRITER.COLLECTOR_MON_PORT
						.getDefaultValue());

		ClientResource clientResource = new ClientResource("http://localhost:"
				+ clientPort + "/collector/shutdown");

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
