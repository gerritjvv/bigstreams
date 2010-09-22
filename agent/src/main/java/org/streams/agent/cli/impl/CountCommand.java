package org.streams.agent.cli.impl;

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
import org.streams.agent.conf.AgentProperties;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.FileTrackingStatusFormatter;
import org.streams.agent.file.FileTrackingStatus.STATUS;
import org.streams.commons.cli.CommandLineProcessor;


/**
 * Implements the count command
 *
 */
@Named("countCommand")
public class CountCommand implements CommandLineProcessor{
	
	private static final Logger LOG = Logger.getLogger(CountCommand.class);
	
	FileTrackingStatusFormatter fileFormatter = new FileTrackingStatusFormatter();
	
	FileTrackerMemory memory;
	org.restlet.Client client;
	Configuration configuration;
	
	public CountCommand(){}
	public CountCommand(FileTrackerMemory memory, org.restlet.Client client , Configuration configuration){
		this.memory = memory;
		this.client = client;
		this.configuration = configuration;
	}
	
	/**
	 * Counts the FileTrackingStatus instances
	 */
	@Override
	public void process(CommandLine cmdLine, OutputStream out) throws Exception {
	
		FileTrackingStatus.STATUS status = null;
		
		String statusStr = cmdLine.getOptionValue("count");
		
		if(!(statusStr == null || statusStr.trim().length() < 1)){
			status = FileTrackingStatus.STATUS.valueOf(statusStr);
		}
		
		PrintWriter writer = new PrintWriter(out);
		try{
			long count = 0L;
			
			if(cmdLine.hasOption("o")){
				LOG.info("Connecting to database directly");
				count = getCountFromDB(status);
			}else{
				LOG.info("Connecting via rest");
				count = getFilesHttp(status);
			}
			writer.print(count);
		}finally{
			writer.close();
		}
		
	}

	/**
	 * Helper method that will call the Rest server via localhost only at
	 * http://localhost:$monitor.port/files/count/$status
	 * 
	 * @param status may be null or not
	 * @return
	 * @throws ResourceException
	 * @throws IOException
	 */
	private long getFilesHttp(FileTrackingStatus.STATUS status) throws ResourceException, IOException {

		int clientPort = configuration.getInt(AgentProperties.MONITORING_PORT,
				8040);

		LOG.info("Connecting client to " + clientPort);

		String querySuffix = (status == null ) ? "/files/count" : "/files/count/" + status.toString().toUpperCase(); 
		ClientResource clientResource = new ClientResource("http://localhost:"
				+ clientPort + querySuffix);

		StringWriter writer = new StringWriter();
		
		try {
			clientResource.get(MediaType.APPLICATION_JSON).write(writer);
		} finally {
			clientResource.release();
		}

		return Integer.valueOf(writer.toString());
	}
	
	/**
	 * Returns the file count using the FileTrackingMemory.
	 * @param status
	 * @return
	 */
	private long getCountFromDB(STATUS status) {

		return (status == null) ? memory.getFileCount() : memory.getFileCount(status); 
		
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
