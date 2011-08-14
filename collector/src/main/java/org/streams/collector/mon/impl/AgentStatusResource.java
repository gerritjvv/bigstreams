package org.streams.collector.mon.impl;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.restlet.Client;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ClientResource;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

/**
 * 
 * Shows the agent status and files
 * 
 */
public class AgentStatusResource extends ServerResource {

	private static final Logger LOG = Logger.getLogger(AgentStatusResource.class);

	private static final String[] STATUSPARAMS = new String[]{"READY", "DONE", "PARKED", "READING"};
	
	ExecutorService executor;
	Client client;
	
	public AgentStatusResource(ExecutorService executor, Client client) {
		super();
		
		this.executor = executor;
		this.client = client;
	}

	@SuppressWarnings("rawtypes")
	@Get("html")
	public Representation getAgentStatusHtml() throws ResourceNotFoundException, ParseErrorException, MethodInvocationException, IOException, Exception{
		
		 
		
		int port = Integer.valueOf(getQuery().getFirstValue("port", "8085"));
		String agentHostParam = getQuery().getFirstValue("agent", true);
		
		if(agentHostParam == null || agentHostParam.trim().length() < 1){
			throw new RuntimeException("Specify the parameters agent and port e.g. ?agent=localhost&post=8085");
		}
		
		agentHostParam = URLDecoder.decode(agentHostParam, "UTF-8");
		agentHostParam = (agentHostParam.equalsIgnoreCase("this")) ? "localhost" + ":" + port : agentHostParam + ":" + port;
		
		System.out.println("------------ host: " + agentHostParam);
		try{
		//work arround for browsers that do not send local host properly.
		final String agentHost = agentHostParam;
		
		LOG.info("using agetHost: " + agentHost);
			
		final VelocityContext ctx = new VelocityContext();
		
		
		Response resp = client.get("http://localhost:8085/files/list/DONE");
		
		ctx.put("DONE",resp.getEntity().getText());
		ctx.put("PARKED", "[]");
		ctx.put("READY", "[]");
		ctx.put("READING", "[]");
//		String statusString = callAgentStatus(agentHost);
//		System.out.println("------- status " + statusString);
//		ctx.put("agentStatus", statusString);
		ctx.put("agentHost", agentHost);
		
		StringWriter writer = new StringWriter();
		Velocity.getTemplate("agentStatusResource.vm").merge(ctx, writer);
		
		return new  StringRepresentation(writer.toString(), MediaType.TEXT_HTML);
		}catch(Throwable t){
			t.printStackTrace();
			System.out.println(getQuery().getFirstValue("agent"));
			return new StringRepresentation("Could not find values for " + getQuery().getFirstValue("agent"));
		}
	}

	private String callAgentStatus(String agentHost) {
		System.out.println("Call agent status: " + "http://" + agentHost + "/agent/status");
		try{
		return doRestCall("http://" + agentHost + "/agent/status");
		}catch(Throwable t){
			LOG.error(t.toString(), t);
			return "ERROR";
		}
	}

	protected String callAgentFilesJson(String agentHost, String statusParam) {
		return doRestCall("http://" + agentHost + "/files/list/" + statusParam);
	}
	
	
	protected String doRestCall(String url) {
		String val = null;
		
		System.out.println("----- REST CALL FOR: " + url);
		ClientResource resource = new ClientResource(url);
		resource.setRetryOnError(true);
		resource.setReference("/files/list/DONE");
		resource.setRetryAttempts(5);
		resource.setMethod(Method.GET);
		try{
			Representation resp = resource.get(MediaType.APPLICATION_JSON);
			
			if(resource.getStatus().isSuccess()){
				try {
					val = resp.getText();
				} catch (IOException e) {
					LOG.error(e.toString(), e);
					val = e.toString();
				}
			}else{
				val = "Error: " + resource.getStatus().getDescription();
				System.out.println("----- error " + val);
			}
		}catch(Throwable t){
		 t.printStackTrace();
		}
		return val;
		
	}
}
