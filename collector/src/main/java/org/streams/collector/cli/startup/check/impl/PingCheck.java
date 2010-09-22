package org.streams.collector.cli.startup.check.impl;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.restlet.Client;
import org.restlet.Response;
import org.streams.collector.conf.CollectorProperties;
import org.streams.commons.app.AbstractStartupCheck;


/**
 * 
 * This check should be executed as a post startup service.<br/>
 * It verifies that the collector ping port is up and running.
 * 
 */
@Named
public class PingCheck extends AbstractStartupCheck {
	
	private static final Logger LOG = Logger.getLogger(PingCheck.class);

	Configuration configuration;
	Client client;
	
	public PingCheck(){}
	
	public PingCheck(Configuration configuration, Client client) {
		this.configuration = configuration;
		this.client = client;
	}

	@Override
	public void runCheck() throws Exception {

		LOG.info("Checking Collector Ping Service");
		
		int port = configuration.getInt(CollectorProperties.WRITER.PING_PORT.toString(),
				(Integer)CollectorProperties.WRITER.PING_PORT.getDefaultValue());
		
		Response resp = client.get("http://localhost:" + port);
		
		checkTrue(resp.getStatus().isSuccess(), 
				resp.getStatus().toString());
		
		LOG.info("DONE");

	}

	public Configuration getConfiguration() {
		return configuration;
	}

	@Inject
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public Client getClient() {
		return client;
	}

	@Inject
	public void setClient(Client client) {
		this.client = client;
	}

}
