package org.streams.collector.mon.impl;

import java.io.StringWriter;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.collector.mon.CollectorStatus;


/**
 * 
 * Implements the Rest Resource for the CollectorStatus implementation.
 * 
 */
public class CollectorStatusResource extends ServerResource {

	CollectorStatus status;

	public CollectorStatusResource(CollectorStatus status) {
		super();
		this.status = status;
	}

	@Get("html")
	public Representation getCollectorsHtml() throws ResourceNotFoundException, ParseErrorException, Exception{
		VelocityContext ctx = new VelocityContext();
		ctx.put("status", getCoordinationStatus());
		StringWriter writer = new StringWriter();
		Velocity.getTemplate("collectorStatusResource.vm").merge(ctx, writer);
		return new  StringRepresentation(writer.toString(), MediaType.TEXT_HTML);
	}

	
	@Get("json")
	public CollectorStatus getCoordinationStatus() {
		return status;
	}

}
