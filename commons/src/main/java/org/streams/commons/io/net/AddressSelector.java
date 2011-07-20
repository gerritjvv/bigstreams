package org.streams.commons.io.net;

import java.net.InetSocketAddress;
import java.util.List;

/**
 *
 *Selects server addresses based on the implementations algorithm
 */
public interface AddressSelector {

	public AddressSelector addAddress(InetSocketAddress socketAddress);
	public AddressSelector removeAddress(InetSocketAddress socketAddress);
	
	public InetSocketAddress nextAddress();
	
	public void setAddresses(List<InetSocketAddress> socketAddress);
	
	public AddressSelector clone();
	
}
