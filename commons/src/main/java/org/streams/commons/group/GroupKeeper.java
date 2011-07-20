package org.streams.commons.group;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

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

	/**
	 * The address selector will be updated dynamically with the addresses of
	 * the agents and collectors in the groups.
	 * 
	 * @param groupType
	 * @param listener
	 */
	void registerAddressSelector(GROUPS groupType, GroupChangeListener listener);

	List<InetSocketAddress> listMembers(GROUPS groupName);

}
