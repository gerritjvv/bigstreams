package org.streams.collector.mon.impl;

import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.commons.group.Group.GroupStatus;
import org.streams.commons.group.GroupKeeper;
import org.streams.commons.group.Group.GroupStatus.ExtraField;
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

	
	@Get("html")
	public Representation getCollectorsHtml() throws ResourceNotFoundException, ParseErrorException, Exception{
		
		List<Map<String, Object>> collectors = getCollectors();
		//TemplateRepresentation rep = new TemplateRepresentation(Velocity.getTemplate("collectorsStatusResource.vm"), model, MediaType.TEXT_HTML);
		VelocityContext ctx = new VelocityContext();
		ctx.put("collectors", collectors);
		StringWriter writer = new StringWriter();
		Velocity.getTemplate("collectorsStatusResource.vm").merge(ctx, writer);
		return new  StringRepresentation(writer.toString(), MediaType.TEXT_HTML);
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
					map.put("lastUpdateDiffHours",
							(System.currentTimeMillis()- stat.getLastUpdate())/ 3600000F
							);
					map.put("lastUpdateDate", dateFormat.format(new Date(stat.getLastUpdate())));
					
					List<ExtraField> extraList = stat.getExtraFieldList();
					if(extraList != null){
						for(ExtraField extraField : extraList)
							map.put(extraField.getKey(), extraField.getValue());
					}
					
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
