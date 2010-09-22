package org.streams.coordination.cli.impl;

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
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.restlet.data.MediaType;
import org.restlet.data.Range;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.streams.commons.cli.CommandLineProcessor;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusFormatter;
import org.streams.commons.file.FileTrackingStatusFormatter.FORMAT;
import org.streams.coordination.CoordinationProperties;
import org.streams.coordination.file.CollectorFileTrackerMemory;


/**
 * Implements the ls command The LS command must connect to the
 * FileTrackingStatusResource which has been started during the start
 * coordination process.<br/>
 * I.e. the current process will not have access to the persistence unit that is
 * being used by the start coordination process and needs to use an out of
 * process call.<br/>
 * <p/>
 * Supports:<br/>
 * <ul>
 *  <li> listing of distinct agent names by providing the -agent parameter. </li>
 *  <li> listing of distinct log types by providing the -logType parameter. </li>
 * </ul> 
 */
@Named("lsCommand")
public class LSCommand implements CommandLineProcessor {

	private static final Logger LOG = Logger.getLogger(LSCommand.class);
	private static final ObjectMapper objectMapper = new ObjectMapper();

	FileTrackingStatusFormatter fileFormatter = new FileTrackingStatusFormatter();

	CollectorFileTrackerMemory memory;
	org.restlet.Client client;
	Configuration configuration;

	FileTrackingStatusFormatter formatter = new FileTrackingStatusFormatter();

	public LSCommand() {
	}

	public LSCommand(CollectorFileTrackerMemory memory,
			org.restlet.Client client, Configuration configuration) {
		this.memory = memory;
		this.client = client;
		this.configuration = configuration;
	}

	/**
	 * Lists the json representation of the FileTrackingStatus instances
	 */
	@Override
	public void process(CommandLine cmdLine, OutputStream out) throws Exception {

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

				if (cmdLine.hasOption("o")) {
					// access the database directly
					LOG.info("Connecting to database directly");
					writeOutput(getFilesDBwithQuery(query, from, max), writer,
							cmdLine);
				} else {
					LOG.info("Connecting via rest");
					writeOutput(getFilesHttp("?query=" + query, from, max),
							writer, cmdLine);
				}

			} else {

				if (cmdLine.hasOption("o")) {
					LOG.info("Connecting to database directly");
					if (cmdLine.hasOption("agent")) {
						writeOutputStrings(getAgentNamesDB(from, max), writer,
								cmdLine);
					}else if ( cmdLine.hasOption("logType")){
						writeOutputStrings(getLogTypeNamesDB(from, max), writer,
								cmdLine);
					}else {
						writeOutput(getFilesDBwithQuery(null, from, max),
								writer, cmdLine);
					}
				} else {
					LOG.info("Connecting via rest");
					if (cmdLine.hasOption("agent")) {
						writeOutputStrings(getItemNamesHttp("/agents/list", from, max),
								writer, cmdLine);
					}else if ( cmdLine.hasOption("logType")){
						writeOutputStrings(getItemNamesHttp("/logTypes/list", from, max),
								writer, cmdLine);
					} else {
						writeOutput(getFilesHttp("", from, max), writer,
								cmdLine);
					}
				}

			}
		} finally {
			writer.close();
		}

	}

	/**
	 * Retrieves the distinct log type names from the database.
	 * @param from
	 * @param max
	 * @return
	 */
	private Collection<String> getLogTypeNamesDB(int from, int max) {
		return memory.getLogTypes(from, max);
	}
	
	/**
	 * Helper method that will call the Rest server via localhost only at
	 * and expect a list of string.
	 * @param addressPath e.g. /agents/list 
	 * @param from
	 * @param max
	 * @return
	 * @throws ResourceExceptionstatus
	 * @throws IOException
	 */
	private Collection<String> getItemNamesHttp(String addressPath, int from, int max)
			throws ResourceException, IOException {

		int clientPort = configuration.getInt(
				CoordinationProperties.PROP.COORDINATION_PORT.toString(),
				(Integer) CoordinationProperties.PROP.COORDINATION_PORT
						.getDefaultValue());

		LOG.info("Connecting client to " + clientPort);

		ClientResource clientResource = new ClientResource("http://localhost:"
				+ clientPort + addressPath);

		StringWriter writer = new StringWriter();
		if (from > -1) {
			clientResource.setRanges(Arrays.asList(new Range(from, max)));
		}
		try {
			clientResource.get().write(writer);
		} finally {
			clientResource.release();
		}

		Collection<String> coll = objectMapper.readValue(writer.toString(),
				new TypeReference<Collection<String>>() {
				});

		return coll;
	}

	/**
	 * Helper method that will call the Rest server via localhost only at
	 * http://localhost:$monitor.port/files/list/$attribute
	 * 
	 * @param attribute
	 * @param from
	 * @param max
	 * @return
	 * @throws ResourceExceptionstatus
	 * @throws IOException
	 */
	private Collection<FileTrackingStatus> getFilesHttp(String attribute,
			int from, int max) throws ResourceException, IOException {

		int clientPort = configuration.getInt(
				CoordinationProperties.PROP.COORDINATION_PORT.toString(),
				(Integer) CoordinationProperties.PROP.COORDINATION_PORT
						.getDefaultValue());

		LOG.info("Connecting client to " + clientPort);

		ClientResource clientResource = new ClientResource("http://localhost:"
				+ clientPort + "/files/list" + attribute);

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
	 * 
	 * @param agents
	 * @param writer
	 * @param cmdLine
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonGenerationException
	 */
	private void writeOutputStrings(Collection<String> agents,
			PrintWriter writer, CommandLine cmdLine)
			throws JsonGenerationException, JsonMappingException, IOException {

		for (String agentName : agents) {
			writer.println(agentName);
		}
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
	 * Gets the agent names from the database
	 * 
	 * @param from
	 * @param max
	 * @return
	 */
	private Collection<String> getAgentNamesDB(int from, int max) {
		if (from < 0) {
			from = 0;
			max = 1000;
		}

		return memory.getAgents(from, max);
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
		if (from < 0) {
			from = 0;
			max = 1000;
		}

		return (query == null) ? memory.getFiles(from, max) : memory
				.getFilesByQuery(query, from, max);

	}

	@Autowired(required=false)
	public void setMemory(CollectorFileTrackerMemory memory) {
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
