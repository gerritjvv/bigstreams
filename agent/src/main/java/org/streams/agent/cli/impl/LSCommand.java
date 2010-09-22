package org.streams.agent.cli.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.restlet.data.MediaType;
import org.restlet.data.Range;
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
 * Implements the ls command The LS command must connect to the
 * FileTrackingStatusResource which has been started during the start Agent
 * process.<br/>
 * I.e. the current process will not have access to the persistence unit that is
 * being used by the start Agent process and needs to use an out of process
 * call.<br/>
 */
@Named("lsCommand")
public class LSCommand implements CommandLineProcessor {

	private static final Logger LOG = Logger.getLogger(LSCommand.class);

	FileTrackingStatusFormatter fileFormatter = new FileTrackingStatusFormatter();

	FileTrackerMemory memory;
	org.restlet.Client client;
	Configuration configuration;

	FileTrackingStatusFormatter formatter = new FileTrackingStatusFormatter();

	public LSCommand() {
	}

	public LSCommand(FileTrackerMemory memory, org.restlet.Client client,
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

		FileTrackingStatus.STATUS status = null;

		if (cmdLine.hasOption("status")) {
			status = FileTrackingStatus.STATUS.valueOf(cmdLine
					.getOptionValue("status"));

		}

		int from = -1;
		int max = 1000;

		if (cmdLine.hasOption("from")) {
			from = Integer.parseInt(cmdLine.getOptionValue("from"));
		}
		if (cmdLine.hasOption("max")) {
			max = Integer.parseInt(cmdLine.getOptionValue("max"));
		}

		PrintWriter writer = new PrintWriter(out);
		try {
			if (cmdLine.hasOption("query")) {
				String query = cmdLine.getOptionValue("query");
				Collection<FileTrackingStatus> files = null;

				if (cmdLine.hasOption("o")) {
					// access the database directly
					LOG.info("Connecting to database directly");
					files = getFilesDBwithQuery(query, from, max);
				} else {
					LOG.info("Connecting via rest");
					files = getFilesHttp("?query=" + query, from, max);
				}
				writeOutput(files, writer, cmdLine);

			} else {
				Collection<FileTrackingStatus> files = null;

				if (cmdLine.hasOption("o")) {
					LOG.info("Connecting to database directly");
					files = getFilesDB(status, from, max);
				} else {
					LOG.info("Connecting via rest");
					String attribute = (status == null) ? "" : status
							.toString();
					files = getFilesHttp(attribute, from, max);
				}

				writeOutput(files, writer, cmdLine);
			}
		} finally {
			writer.close();
		}

	}

	/**
	 * Helper method that will call the Rest server via localhost only at
	 * http://localhost:$monitor.port/files/list/$attribute
	 * 
	 * @param attribute
	 * @param from
	 * @param max
	 * @return
	 * @throws ResourceException
	 * @throws IOException
	 */
	private Collection<FileTrackingStatus> getFilesHttp(String attribute,
			int from, int max) throws ResourceException, IOException {

		int clientPort = configuration.getInt(AgentProperties.MONITORING_PORT,
				8040);

		LOG.info("Connecting client to " + clientPort);

		String suffix = "/files/list";
		if(attribute != null && attribute.length() > 0){
			suffix += "/" + attribute;
		}
		ClientResource clientResource = new ClientResource("http://localhost:"
				+ clientPort + suffix);

		StringWriter writer = new StringWriter();
		if (from > -1) {
			clientResource.setRanges(Arrays.asList(new Range(from, max)));
		}
		try {
			clientResource.get(MediaType.APPLICATION_JSON).write(writer);
		} finally {
			clientResource.release();
		}

		return formatter.readList(FORMAT.JSON,
				new StringReader(writer.toString()));
	}

	/**
	 * Supports querying the storage
	 * 
	 * @param writer
	 * @param cmdLine
	 * @param status
	 * @param from
	 * @param max
	 */
	private void writeOutput(Collection<FileTrackingStatus> files,
			PrintWriter writer, CommandLine cmdLine) {

		if (cmdLine.hasOption("json")) {
			String line = null;
			for (FileTrackingStatus file : files) {
				line = fileFormatter.write(
						FileTrackingStatusFormatter.FORMAT.JSON, file);
				writer.println(line);
			}
		} else {
			for (FileTrackingStatus file : files) {
				String line = fileFormatter.write(
						FileTrackingStatusFormatter.FORMAT.TXT, file);
				writer.println(line);
			}
		}
	}

	/**
	 * Gets the files from the database
	 * 
	 * @param status
	 * @param from
	 * @param max
	 * @return
	 */
	private Collection<FileTrackingStatus> getFilesDBwithQuery(String query,
			int from, int max) {
		Collection<FileTrackingStatus> files;
		if (from > -1) {
			files = memory.getFiles(query, from, max);
		} else {
			files = memory.getFiles(query, 0, 1000);
		}
		return files;
	}

	/**
	 * Gets the files from the database
	 * 
	 * @param status
	 * @param from
	 * @param max
	 * @return
	 */
	private Collection<FileTrackingStatus> getFilesDB(
			FileTrackingStatus.STATUS status, int from, int max) {
		Collection<FileTrackingStatus> files;
		if (from > -1) {
			files = memory.getFiles(status, from, max);
		} else {
			files = memory.getFiles(status);
		}
		return files;
	}

	@Autowired(required=false)
	public void setMemory(FileTrackerMemory memory) {
		this.memory = memory;
	}

	@Autowired(required=false)
	public void setClient(org.restlet.Client client) {
		this.client = client;
	}

	@Inject
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}
}
