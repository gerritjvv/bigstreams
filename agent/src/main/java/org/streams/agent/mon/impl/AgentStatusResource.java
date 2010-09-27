package org.streams.agent.mon.impl;

import javax.inject.Inject;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.agent.mon.status.AgentStatus;

/**
 */
public class AgentStatusResource extends ServerResource {

	AgentStatus status;

	@Get("json")
	public AgentStatus getAgentStatus() {
		return status;
	}

	@Inject
	public void setStatus(AgentStatus status) {
		this.status = status;
	}

}
