package org.streams.gring.group.impl;

import java.net.InetSocketAddress;

import org.streams.gring.group.MemberDesc;

/**
 * 
 * Describes a Member in the group ring
 * <p/>
 * MemberDesc identity is based only on the Member id. This is a sha1 generated
 * hash base on the member's ip address.
 */
public class MemberDescImpl implements MemberDesc, Comparable<MemberDesc> {

	long id;
	InetSocketAddress inetAddress;

	public MemberDescImpl(long id, InetSocketAddress inetAddress) {
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

	public InetSocketAddress getInetAddress() {
		return inetAddress;
	}

	public void setInetAddress(InetSocketAddress inetAddress) {
		this.inetAddress = inetAddress;
	}

	public String toString() {
		return "MemberDesc[id: " + id + "; address: "
				+ inetAddress.getHostName() + "]";
	}

	@Override
	public int compareTo(MemberDesc o) {
		long otherId = o.getId();

		if (otherId < id) {
			return 1;
		} else if (otherId > id) {
			return -1;
		} else {
			return 0;
		}

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MemberDescImpl other = (MemberDescImpl) obj;
		if (id != other.id)
			return false;
		return true;
	}

}
