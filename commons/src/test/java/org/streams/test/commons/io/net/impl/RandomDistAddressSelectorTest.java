package org.streams.test.commons.io.net.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.commons.io.net.AddressSelector;
import org.streams.commons.io.net.impl.RandomDistAddressSelector;

/**
 * 
 * Tests the basic workings of the RandomDistAddressSelector
 * 
 */
public class RandomDistAddressSelectorTest extends TestCase {

	@Test
	public void testRandom() throws InterruptedException {

		final AddressSelector select = new RandomDistAddressSelector(
				new InetSocketAddress[] {
						new InetSocketAddress("localhsot", 80),
						new InetSocketAddress("localhsot", 80),
						new InetSocketAddress("localhsot", 80)

				});

		
		
		int count = 10;
		final CountDownLatch latch = new CountDownLatch(count);
		
		for(int i = 0; i < count; i++){
			new Thread(){
				public void run(){
		
					try{
					InetSocketAddress address;
					AddressSelector localSelector = select;
					while( (address = localSelector.nextAddress()) != null){
						localSelector = localSelector.clone().removeAddress(address);
					}
				
					}catch(Throwable t){
						t.printStackTrace();
					}
					
					latch.countDown();
				}
				
			}.start();
		
		}
		
		latch.await();
	}

}
