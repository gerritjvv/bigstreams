package org.streams.agent.cli.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import javax.inject.Named;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.restlet.data.MediaType;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.streams.agent.conf.AgentProperties;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.FileTrackingStatusFormatter;
import org.streams.agent.file.FileTrackingStatusFormatter.FORMAT;
import org.streams.commons.cli.CommandLineProcessor;


/**
 * Implements the status command
 * 
 */
@Named("statusCommand")
public class StatusCommand implements CommandLineProcessor {

	private static final Logger LOG = Logger.getLogger(CountCommand.class);

	FileTrackerMemory memory;
	org.restlet.Client client;
	Configuration configuration;

	FileTrackingStatusFormatter fileFormatter = new FileTrackingStatusFormatter();

	public StatusCommand() {
	}

	public StatusCommand(FileTrackerMemory memory, org.restlet.Client client,
			Configuration configuration) {
		this.memory = memory;
		this.client = client;
		this.configuration = configuration;
	}

	/**
	 * Lists the json representation of the FileTrackingStatus instances
	 */
	@Override
	public void process(CommandLine cmdLine, OutputStream out) throws Exception {

		String path = cmdLine.getOptionValue("status");

		if (path == null || path.trim().length() < 1) {
			throw new RuntimeException(
					"Please provide a path argument for status");
		}

		PrintWriter writer = new PrintWriter(out);
		try {

			FileTrackingStatus file = null;

			if (cmdLine.hasOption("o")) {
				LOG.info("Connecting to database directly");
				file = memory.getFileStatus(new File(path));
			} else {
				LOG.info("Connecting via rest");
				file = getFileStatusHttp(path);
			}

			if (file == null) {
				throw new FileNotFoundException(path);
			}

			if (cmdLine.hasOption("json")) {
				String json = fileFormatter.write(
						FileTrackingStatusFormatter.FORMAT.JSON, file);
				writer.println(json);
			} else {
				String line = fileFormatter.write(
						FileTrackingStatusFormatter.FORMAT.TXT, file);
				writer.println(line);

			}

		} finally {
			writer.close();
		}

	}

	/**
	 * calls the Rest service registerd at localhost:[port]/files/status/{path}
	 * 
	 * @param path
	 * @return
	 * @throws ResourceException
	 * @throws IOException
	 */
	private FileTrackingStatus getFileStatusHttp(String path)
			throws ResourceException, IOException {
		int clientPort = configuration.getInt(AgentProperties.MONITORING_PORT,
				8040);

		LOG.info("Connecting client to " + clientPort);

		String absolutePath = path;

		if (!path.startsWith("/")) {
			absolutePath = "/" + path;
		}

		// query the list resource path query for the path
		ClientResource clientResource = new ClientResource("http://localhost:"
				+ clientPort + "/files/status" + absolutePath);

		StringWriter writer = new StringWriter();

		try {
			clientResource.get(MediaType.APPLICATION_JSON).write(writer);
		} finally {
			clientResource.release();
		}

		String content = writer.toString();

		return (content == null || content.trim().length() < 1) ? null
				: fileFormatter.read(FORMAT.JSON, content);

	}

	@Autowired(required = false)
	public void setMemory(FileTrackerMemory memory) {
		this.memory = memory;
	}

	@Autowired(required = false)
	public void setClient(org.restlet.Client client) {
		this.client = client;
	}

}
