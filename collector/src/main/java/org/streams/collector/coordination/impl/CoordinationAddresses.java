package org.streams.collector.coordination.impl;

import org.streams.commons.io.net.AddressSelector;

/**
 *
 * Contains the coordination AddressSelector(s)
 *
 */
public class CoordinationAddresses {

	AddressSelector lockAddressSelector;
	AddressSelector unlockAddressSelector;
	
	
	public CoordinationAddresses(AddressSelector lockAddressSelector,
			AddressSelector unlockAddressSelector) {
		super();
		this.lockAddressSelector = lockAddressSelector;
		this.unlockAddressSelector = unlockAddressSelector;
	}
	
	public AddressSelector getLockAddressSelector() {
		return lockAddressSelector;
	}
	public AddressSelector getUnlockAddressSelector() {
		return unlockAddressSelector;
	}
	
	
}
