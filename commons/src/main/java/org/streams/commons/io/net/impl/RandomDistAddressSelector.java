package org.streams.commons.io.net.impl;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import org.streams.commons.io.net.AddressSelector;

/**
 * 
 * Selects the next address based on the mod and a Random number.<br/>
 * e.g. address = [ ]<br/>
 * nextAddress = address[ Random.nextInt() % address.size() ]<br/>
 * <p/>
 * Using random gives a good event spread of usage over all addresses
 * 
 */
public class RandomDistAddressSelector implements AddressSelector {

	private List<InetSocketAddress> addresses;
	private final Random random = new Random();

	public RandomDistAddressSelector() {
		addresses = new CopyOnWriteArrayList<InetSocketAddress>();
	}

	public RandomDistAddressSelector(Collection<InetSocketAddress> addresses) {
		this.addresses = new CopyOnWriteArrayList<InetSocketAddress>(addresses);
	}

	public RandomDistAddressSelector(InetSocketAddress... addresses) {
		this.addresses = new CopyOnWriteArrayList<InetSocketAddress>(addresses);
	}

	@Override
	public AddressSelector addAddress(InetSocketAddress socketAddress) {
		addresses.add(socketAddress);
		return this;
	}

	@Override
	public AddressSelector removeAddress(InetSocketAddress socketAddress) {
		addresses.remove(socketAddress);
		return this;
	}

	@Override
	public InetSocketAddress nextAddress() {

		InetSocketAddress address = null;

		if (addresses.size() > 0) {
			int index = random.nextInt() % (addresses.size());
			if (index < 0) {
				index *= -1;
			}

			address = addresses.get(index);
		}
		return address;
	}

	@Override
	public AddressSelector clone() {
		return new RandomDistAddressSelector(addresses);
	}

	public String toString(){
		StringBuilder buff = new StringBuilder();
		buff.append("addresses[ ");
		if(addresses != null){
			int i = 0;
			for(InetSocketAddress address: addresses){
				if(i++ != 0) buff.append(",");
				
				buff.append(address);
			}
		}
		
		buff.append(" ]");
		
		return buff.toString();
	}
}
