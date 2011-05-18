package org.streams.coordination.mon.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.streams.coordination.cli.startup.service.impl.HazelcastStartupService;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

/**
 * 
 * Shows the cluster members that this coordination service is part of.
 * 
 */
public class CoordinationClusterResource extends ServerResource {

	HazelcastStartupService startupService;

	public CoordinationClusterResource(HazelcastStartupService startupService) {
		super();
		this.startupService = startupService;
	}

	@Get("json")
	public Collection<String> getCoordinationStatus() {

		Collection<String> coll = new ArrayList<String>();
		HazelcastInstance instance = startupService.getHazelcastInstance();

		Set<Member> members = instance.getCluster().getMembers();
		if (members != null) {
			for (Member member : members) {
				coll.add(member.getInetSocketAddress().toString());
			}
		}

		return coll;
	}

}
