package org.streams.coordination.cli.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.restlet.data.MediaType;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.streams.commons.cli.CommandLineProcessor;
import org.streams.commons.file.FileTrackingStatusFormatter;
import org.streams.coordination.CoordinationProperties;
import org.streams.coordination.file.CollectorFileTrackerMemory;


/**
 * Implements the count command
 * 
 */
@Named("countCommand")
public class CountCommand implements CommandLineProcessor {

	private static final Logger LOG = Logger.getLogger(CountCommand.class);

	FileTrackingStatusFormatter fileFormatter = new FileTrackingStatusFormatter();

	CollectorFileTrackerMemory memory;
	org.restlet.Client client;
	Configuration configuration;

	public CountCommand() {
	}

	public CountCommand(CollectorFileTrackerMemory memory,
			org.restlet.Client client, Configuration configuration) {
		this.memory = memory;
		this.client = client;
		this.configuration = configuration;
	}

	/**
	 * Counts the FileTrackingStatus instances
	 */
	@Override
	public void process(CommandLine cmdLine, OutputStream out) throws Exception {

		String query = null;

		if (cmdLine.hasOption("query")) {
			query = cmdLine.getOptionValue("query");
		}

		PrintWriter writer = new PrintWriter(out);
		try {
			long count = 0L;

			if (cmdLine.hasOption("o")) {
				LOG.info("Connecting to database directly");

				if (query != null) {
					count = memory.getFileCountByQuery(query);
				} else if (cmdLine.hasOption("agent")) {
					count = memory.getAgentCount();
				} else if (cmdLine.hasOption("logType")) {
					count = memory.getLogTypeCount();
				} else {
					count = memory.getFileCount();
				}

			} else {

				String querySuffix = null;

				if (query != null) {
					querySuffix = "/files/count?query=" + query;
				} else if (cmdLine.hasOption("agent")) {
					querySuffix = "/agents/count";
				} else if (cmdLine.hasOption("logType")) {
					querySuffix = "/logTypes/count";
				} else {
					querySuffix = "/files/count";
				}

				LOG.info("Connecting via rest");
				count = getFilesHttp(querySuffix);
			}
			writer.println(count);
		} finally {
			writer.close();
		}

	}

	/**
	 * Helper method that will call the Rest server via localhost only at
	 * 
	 * @param querySuffix
	 * @return
	 * @throws ResourceException
	 * @throws IOException
	 */
	private long getFilesHttp(String querySuffix) throws ResourceException,
			IOException {

		int clientPort = configuration.getInt(
				CoordinationProperties.PROP.COORDINATION_PORT.toString(),
				(Integer) CoordinationProperties.PROP.COORDINATION_PORT
						.getDefaultValue());

		LOG.info("Connecting client to " + clientPort);

		ClientResource clientResource = new ClientResource("http://localhost:"
				+ clientPort + querySuffix);

		StringWriter writer = new StringWriter();

		try {
			clientResource.get(MediaType.APPLICATION_JSON).write(writer);
		} finally {
			clientResource.release();
		}

		return Long.valueOf(writer.toString());
	}

	@Autowired(required = false)
	@Named("collectorFileTrackerMemory")
	public void setMemory(CollectorFileTrackerMemory memory) {
		this.memory = memory;
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
