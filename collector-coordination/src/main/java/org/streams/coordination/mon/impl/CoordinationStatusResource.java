package org.streams.coordination.mon.impl;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.coordination.mon.CoordinationStatus;


/**
 * 
 * Implements the Rest Resource for the CoordinationStatus implementation.
 * 
 */
public class CoordinationStatusResource extends ServerResource {

	CoordinationStatus status;
	

	public CoordinationStatusResource(CoordinationStatus status) {
		super();
		this.status = status;
	}

	@Get("json")
	public CoordinationStatus getCoordinationStatus() {
		return status;
	}
	
	


}
