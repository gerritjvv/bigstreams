package org.streams.collector.mon.impl;

import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.restlet.Client;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

/**
 * 
 * Shows the agent status and files
 * 
 */
public class AgentStatusResource extends ServerResource {

	private static final Logger LOG = Logger.getLogger(AgentStatusResource.class);

	
	ExecutorService executor;
	Client client;
	
	public AgentStatusResource(ExecutorService executor, Client client) {
		super();
		
		this.executor = executor;
		this.client = client;
	}

	@Get("html")
	public Representation getAgentStatusHtml() throws ResourceNotFoundException, ParseErrorException, MethodInvocationException, IOException, Exception{
		
		 
		
		int port = Integer.valueOf(getQuery().getFirstValue("port", "8085"));
		String agentHostParam = getQuery().getFirstValue("agent", true);
		
		if(agentHostParam == null || agentHostParam.trim().length() < 1){
			throw new RuntimeException("Specify the parameters agent and port e.g. ?agent=localhost&post=8085");
		}
		
		
		try{
		//work arround for browsers that do not send local host properly.
		final String agentHost = agentHostParam + ":" + port;
		
		LOG.info("using agetHost: " + agentHost);
			
		final VelocityContext ctx = new VelocityContext();
		
		
		String doneText = getFiles("http://" + agentHost + "/files/list/DONE");
		String readyText = getFiles("http://" + agentHost + "/files/list/READY");
		String parkedText = getFiles("http://" + agentHost + "/files/list/PARKED");
		String readingText = getFiles("http://" + agentHost + "/files/list/READING");
		String readErrorText = getFiles("http://" + agentHost + "/files/list/READ_ERROR");
		String deletedText = getFiles("http://" + agentHost + "/files/list/DELETED");
		
		
		String agentStatus = getFiles("http://" + agentHost + "/agent/status");
		
		ctx.put("DONE", doneText);
		ctx.put("PARKED", parkedText);
		ctx.put("READY", readyText);
		ctx.put("READING", readingText);
		ctx.put("DELETED", deletedText);
		ctx.put("READ_ERROR", readErrorText);
		ctx.put("agentStatus", agentStatus);
		ctx.put("agentHost", agentHost);
		 
		StringWriter writer = new StringWriter();
		Velocity.getTemplate("agentStatusResource.vm").merge(ctx, writer);
		
		return new  StringRepresentation(writer.toString(), MediaType.TEXT_HTML);
		}catch(Throwable t){
			t.printStackTrace();
			System.out.println(getQuery().getFirstValue("agent"));
			return new StringRepresentation("Could not find values for " + getQuery().getFirstValue("agent") + " : " + t.toString() + " Ensure that the agent is reacheable from the collector.");
		}
	}

	private String getFiles(String url) {
		
		Response resp = client.get(url);
		
		if(resp.getStatus().isSuccess()){
		
			return resp.getEntityAsText();
		}else{
			throw new RuntimeException("Error: " + resp.getStatus().getDescription());
		}
		
	}
	
}
