package org.streams.gring.group;

import java.util.SortedSet;

/**
 * Each member in the group ring uses the GRing instance to keep a snapshot of
 * the current network layout.
 * 
 */
public interface GRing {

	MemberDesc getPredecessor();

	MemberDesc getSuccessor();

	void blackListMember(MemberDesc member);
	
	/**
	 * Return a concurrently accessible member sorted member set excluding the
	 * current member. The first item will be the successor and the last element
	 * the predecessor.
	 * 
	 * @return
	 */
	SortedSet<MemberDesc> getMembers();

	
	boolean hasMembers();
	
}
