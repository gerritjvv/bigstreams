package org.streams.commons.zookeeper;

/**
 * 
 * A group has a name and type.
 *
 */
public class ZGroupMember {

	enum TYPE { AGENT, COLLECTOR }
	enum STATUS { OK, ERROR }
	
	TYPE type;
	String hostname;
	STATUS status;
	
	int load;
	
	
}
