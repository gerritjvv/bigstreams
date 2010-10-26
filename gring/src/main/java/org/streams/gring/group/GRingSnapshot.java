package org.streams.gring.group;

import java.util.SortedSet;

/**
 * 
 *  A snapshot of the current state of the group ring.
 *
 */
public interface GRingSnapshot {
	
	MemberDesc getSuccessor();
	MemberDesc getPredecessor();
	
	SortedSet<MemberDesc> getMembers();
	void add(MemberDesc member);
	
}
