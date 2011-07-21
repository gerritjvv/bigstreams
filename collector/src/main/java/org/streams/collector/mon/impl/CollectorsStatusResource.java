package org.streams.collector.mon.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.commons.group.Group.GroupStatus;
import org.streams.commons.group.GroupKeeper;
import org.streams.commons.group.GroupKeeper.GROUPS;

/**
 * 
 * Implements the Rest Resource that shows the collectors in the GroupKeeker
 * 
 */
public class CollectorsStatusResource extends ServerResource {

	private static final Logger LOG = Logger
			.getLogger(CollectorsStatusResource.class);

	GroupKeeper groupKeeper;

	public CollectorsStatusResource(GroupKeeper groupKeeper) {
		super();
		this.groupKeeper = groupKeeper;
	}

	@Get("json")
	public List<Map<String, Object>> getCollectors() {

		List<Map<String, Object>> propertyColl = new ArrayList<Map<String, Object>>();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:MM:ss");
		try {

			Collection<GroupStatus> stats = groupKeeper
					.listStatus(GROUPS.COLLECTORS);

			if (stats != null) {

				for (GroupStatus stat : stats) {
					Map<String, Object> map = new HashMap<String, Object>();
					map.put("host", stat.getHost());
					map.put("port", stat.getPort());
					map.put("status", stat.getStatus());
					map.put("msg", stat.getMsg());
					map.put("lastUpdate", stat.getLastUpdate());
					
					map.put("lastUpdateDate", dateFormat.format(new Date(stat.getLastUpdate())));
					
					propertyColl.add(map);
				}
			}

			return propertyColl;

		} catch (Throwable t) {
			LOG.error(t.toString(), t);
			return propertyColl;
		}
	}


}
