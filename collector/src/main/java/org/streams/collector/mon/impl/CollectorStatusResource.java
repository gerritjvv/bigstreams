package org.streams.collector.mon.impl;

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

	@Get("json")
	public CollectorStatus getCoordinationStatus() {
		return status;
	}

}
