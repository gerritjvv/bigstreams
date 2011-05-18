package org.streams.agent.mon.impl;

import java.util.HashMap;
import java.util.Map;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.agent.conf.AgentConfiguration;
import org.streams.agent.conf.LogDirConf;
import org.streams.agent.file.actions.LogActionsConf;

/**
 * Displays the configuration of the agent instance
 */
public class AgentConfigResource extends ServerResource {

	AgentConfiguration agentConfig;
	LogDirConf logDirConf;
	LogActionsConf logActionsConf;
//	
	public AgentConfigResource(AgentConfiguration agentConfig,
			LogDirConf logDirConf, LogActionsConf logActionsConf) {
		super();
		this.agentConfig = agentConfig;
		this.logDirConf = logDirConf;
		this.logActionsConf = logActionsConf;
	}

	@Get("json")
	public Map<String, Object> getAgentConfig() {
		Map<String, Object> configMap = new HashMap<String, Object>();
		
		configMap.put("agent", agentConfig.toMap());
		configMap.put("logDir", logDirConf.toMap());
		configMap.put("logActions", logActionsConf.toMap());
		
		return configMap;
	}

	public void setAgentConfig(AgentConfiguration agentConfig) {
		this.agentConfig = agentConfig;
	}

	


}
