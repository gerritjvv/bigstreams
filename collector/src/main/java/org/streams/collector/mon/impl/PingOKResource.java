package org.streams.collector.mon.impl;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

/**
 * A restlet that will always return OK
 * 
 */
public class PingOKResource extends ServerResource {

	@Get("txt")
	public String toString() {
		return "OK";
	}


}
