package org.streams.commons.io.net;

import java.net.InetSocketAddress;

/**
 *
 *Selects server addresses based on the implementations algorithm
 */
public interface AddressSelector {

	public AddressSelector addAddress(InetSocketAddress socketAddress);
	public AddressSelector removeAddress(InetSocketAddress socketAddress);
	
	public InetSocketAddress nextAddress();
	
	public AddressSelector clone();
	
}
