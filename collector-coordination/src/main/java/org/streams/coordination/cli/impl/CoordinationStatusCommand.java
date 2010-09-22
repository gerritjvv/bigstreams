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
import org.codehaus.jackson.map.ObjectMapper;
import org.restlet.data.MediaType;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;
import org.springframework.beans.factory.annotation.Autowired;
import org.streams.commons.cli.CommandLineProcessor;
import org.streams.coordination.CoordinationProperties;
import org.streams.coordination.mon.CoordinationStatus;
import org.streams.coordination.mon.impl.CoordinationStatusImpl;


/**
 * Implements the CCordinationStatus Command command
 * 
 */
@Named("coordinationStatusCommand")
public class CoordinationStatusCommand implements CommandLineProcessor {

	private static final Logger LOG = Logger.getLogger(CoordinationStatusCommand.class);

	private static final ObjectMapper objMapper = new ObjectMapper();
	
	org.restlet.Client client;
	Configuration configuration;

	public CoordinationStatusCommand() {
	}

		/**
	 * Counts the FileTrackingStatus instances
	 */
	@Override
	public void process(CommandLine cmdLine, OutputStream out) throws Exception {
		
		
		PrintWriter writer = new PrintWriter(out);
		if(client == null){
			//if no client is injected print out shutdown
			writer.println("shutdown");
		}else{
			//print status out in json
			String status = getStatusHttp();
			writer.println(status);
		}
		
		writer.close();
	}

	/**
	 * 
	 * @return
	 * @throws ResourceException
	 * @throws IOException
	 */
	private String getStatusHttp() throws ResourceException,
			IOException {

		int clientPort = configuration.getInt(
				CoordinationProperties.PROP.COORDINATION_PORT.toString(),
				(Integer) CoordinationProperties.PROP.COORDINATION_PORT
						.getDefaultValue());

		LOG.info("Connecting client to " + clientPort);

		
		ClientResource clientResource = new ClientResource("http://localhost:"
				+ clientPort + "/coordination/status");

		StringWriter writer = new StringWriter();

		try {
			clientResource.get(MediaType.APPLICATION_JSON).write(writer);
		}catch(ResourceException rexp){
			
			rexp.printStackTrace(System.err);
			
			CoordinationStatusImpl status = new CoordinationStatusImpl();
			status.setStatus(CoordinationStatus.STATUS.SHUTDOWN, rexp.getStatus().toString());
			
			objMapper.writeValue(writer, status);
			
		}finally {
			clientResource.release();
		}
		
		return writer.toString();
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
