package org.streams.gring.group.impl;

import java.util.SortedSet;

import org.streams.gring.group.GRingSnapshot;
import org.streams.gring.group.MemberDesc;

/**
 * 
 *  Store the MemberDesc instances in a SortedSet
 *
 */
public class GRingSnapshotImpl implements GRingSnapshot{

	
	SortedSet<MemberDesc> members;
	
	public GRingSnapshotImpl(SortedSet<MemberDesc> members) {
		super();
		this.members = members;
	}

	@Override
	public MemberDesc getSuccessor() {
		return members.first();
	}

	@Override
	public MemberDesc getPredecessor() {
		return members.last();
	}

	@Override
	public SortedSet<MemberDesc> getMembers() {
		return members;
	}
	
	public void add(MemberDesc member){
		members.add(member);
	}

}
