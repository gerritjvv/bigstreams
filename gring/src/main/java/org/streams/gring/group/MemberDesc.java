package org.streams.gring.group;

import java.net.InetSocketAddress;

/**
 * Each member in the group ring has a description. The description is made up
 * of an ID and the Member's host name.
 * 
 */
public interface MemberDesc {

	long getId();

	InetSocketAddress getInetAddress();

}
