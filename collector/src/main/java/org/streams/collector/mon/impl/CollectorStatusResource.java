package org.streams.collector.mon.impl;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.codehaus.jackson.map.ObjectMapper;
import org.restlet.Client;
import org.restlet.data.MediaType;
import org.restlet.data.Protocol;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.ClientResource;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;
import org.streams.collector.mon.CollectorStatus;


/**
 * 
 * Implements the Rest Resource for the CollectorStatus implementation.
 * 
 */
public class CollectorStatusResource extends ServerResource {

	private static final ObjectMapper objectMapper = new ObjectMapper();
	CollectorStatus status;
	Client client;
	int viewPort;
	
	public CollectorStatusResource(int viewPort, CollectorStatus status, Client client) {
		super();
		this.viewPort = viewPort;
		this.status = status;
		this.client = client;
		
	}

	
	@Get("html")
	public Representation getCollectorsHtml() throws ResourceNotFoundException, ParseErrorException, Exception{
		
		//this is not the view port
		int port = Integer.valueOf(getQuery().getFirstValue("port", "8220"));
		String collectorHostParam = getQuery().getFirstValue("collector", true);
		String statusObj = null;
		
		if(collectorHostParam != null && collectorHostParam.trim().length() > 0){
			String url = "http://" + collectorHostParam + ":" + viewPort + "/view/collector/status";
			getResponse().redirectPermanent(url);
			return null;
		}else{
			statusObj = objectMapper.writeValueAsString(status);
			collectorHostParam = "localhost";
		}
		
		VelocityContext ctx = new VelocityContext();
		
		ctx.put("collectorPort", port);
		ctx.put("collectorHost", collectorHostParam);
		ctx.put("collectorStatus", statusObj);
		StringWriter writer = new StringWriter();
		Velocity.getTemplate("collectorStatusResource.vm").merge(ctx, writer);
		
		return  new  StringRepresentation(writer.toString(), MediaType.TEXT_HTML);
	}

	
	@Get("json")
	public CollectorStatus getCoordinationStatus() {
		return status;
	}

}
