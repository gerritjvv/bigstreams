package org.streams.gring.transmit.impl;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

import org.streams.gring.group.GRing;
import org.streams.gring.group.MemberDesc;

/**
 * A simple mock GRing 
 *
 */
public class MockGRing implements GRing{

	SortedSet<MemberDesc> testMembers;
	
	public MockGRing(MemberDesc... members){
		
		testMembers = new TreeSet<MemberDesc>(
				Arrays.asList(members)
				);
		
		
	}
	
	@Override
	public MemberDesc getPredecessor() {
		// TODO Auto-generated method stub
		return testMembers.last();
	}

	@Override
	public MemberDesc getSuccessor() {
		// TODO Auto-generated method stub
		return testMembers.first();
	}

	@Override
	public void blackListMember(MemberDesc member) {
		testMembers.remove(member);
	}

	@Override
	public SortedSet<MemberDesc> getMembers() {
		return testMembers;
	}

	@Override
	public boolean hasMembers() {
		return testMembers.size() > 0;
	}

}
