package org.streams.agent.mon.impl;

import javax.inject.Inject;
import javax.inject.Named;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.agent.mon.AgentStatus;


/**
 */
@Named("agentStatusResource")
public class AgentStatusResource extends ServerResource {

	AgentStatus status;


	@Inject
	public AgentStatusResource(AgentStatus status) {
		super();
		this.status = status;
	}


	@Get("json")
	public AgentStatus getAgentStatus() {
		return status;
	}



}
