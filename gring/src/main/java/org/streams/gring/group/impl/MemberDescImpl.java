package org.streams.gring.group.impl;

import java.net.InetAddress;

import org.streams.gring.group.MemberDesc;

/**
 * 
 *  Describes a Member in the group ring
 *
 */
public class MemberDescImpl implements MemberDesc, Comparable<MemberDesc>{

	
	long id;
	InetAddress inetAddress;
	
	public MemberDescImpl(long id, InetAddress inetAddress) {
		super();
		this.id = id;
		this.inetAddress = inetAddress;
	}
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public InetAddress getInetAddress() {
		return inetAddress;
	}
	public void setInetAddress(InetAddress inetAddress) {
		this.inetAddress = inetAddress;
	}
	
	public String toString(){
		return "MemberDesc[id: " + id + "; address: " + inetAddress.getHostName() + "]";
	}

	@Override
	public int compareTo(MemberDesc o) {
		long otherId = o.getId();
		
		if(otherId < id){
			return 1;
		}else if(otherId > id){
			return -1;
		}else{
			return 0;
		}
		
	}
	
}
