package org.streams.commons.group;

import java.util.Collection;

/**
 * 
 * Implementations provide the group management implementation.
 * 
 */
public interface GroupKeeper {

	enum GROUPS {
		COLLECTORS, AGENTS
	}
	
	Collection<Group.GroupStatus> listStatus(GROUPS groupName);
	
	Collection<GROUPS> listGroups();
	
	void updateStatus(Group.GroupStatus status);
	
}
