package org.streams.collector.mon.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

/**
 * 
 * Shows the collector configuration
 */
public class CollectorConfigResource extends ServerResource {

	Configuration config;

	public CollectorConfigResource(Configuration config) {
		super();
		this.config = config;
	}

	@SuppressWarnings("unchecked")
	@Get("json")
	public Map<String, String> getConfiguration() {

		Map<String, String> map = new HashMap<String, String>();

		Iterator<String> it = config.getKeys();
		while (it.hasNext()) {
			String key = it.next();
			Object val = config.getProperty(key);
			String strVal = (val == null) ? "null" : val.toString();

			map.put(key, strVal);

		}

		return map;
	}

}
