package org.streams.gring.transmit.impl;

import java.net.InetAddress;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.gring.group.error.NoGRingMemebersException;
import org.streams.gring.group.impl.GRingSnapshotImpl;
import org.streams.gring.group.impl.MemberDescImpl;
import org.streams.gring.message.Message;
import org.streams.gring.message.MessageTransmitListener;
import org.streams.gring.message.impl.MessageImpl;
import org.streams.gring.net.GRingClient;
import org.streams.gring.net.GRingClientFactory;

/**
 * 
 *  Test the TransmitterThread
 *
 */
public class TransmitterThreadTest extends TestCase {

	int messagesPerChannel = 10;
	BlockingDeque<Message> messageQueue = new LinkedBlockingDeque<Message>();
	
	/**
	 * Test that what happens if when an error is induced on sending.
	 * @throws Exception
	 */
	@Test
	public void testMemberBlaclist() throws Exception{
		
		
		GRingClientFactory factory = createFactory(true);
		
		MockGRing gring = new MockGRing(new MemberDescImpl(1L, InetAddress.getLocalHost()));
		TransmitterThread transmitterThread = new TransmitterThread(gring, messageQueue, messagesPerChannel, factory);
		
		Thread th = new Thread(transmitterThread);
		th.start();
		
		final AtomicBoolean errorFound = new AtomicBoolean(false);
		
		try{
			final AtomicBoolean messageSent = new AtomicBoolean(false);
			
			/**
			 * GRingSnapshot gring,
				MessageReceiptListener messageReceiptListener,
				MessageTransmitListener messageTransmitListener
			 */
			
			//now add a message to the queue
			Message message = new MessageImpl(1L, Message.TYPE.WRITE, new GRingSnapshotImpl(null), null, new MessageTransmitListener(){
	
				@Override
				public void messageSent(Message request) {
					messageSent.set(true);
				}
	
				@Override
				public void error(Message request, Throwable t) {
					errorFound.set(true);
					assertTrue( t instanceof NoGRingMemebersException);
				}
				
			});
			
			
			messageQueue.add(message);
			
			Thread.sleep(500L);
			
			assertTrue(errorFound.get());
		}finally{
			th.interrupt();
		}
		
	}
	
	
	/**
	 * Send a simple message.
	 * @throws Exception
	 */
	@Test
	public void testSendMessage() throws Exception{
		
		GRingClientFactory factory = createFactory(false);
		MockGRing gring = new MockGRing(new MemberDescImpl(1L, InetAddress.getLocalHost()));
		TransmitterThread transmitterThread = new TransmitterThread(gring, messageQueue, messagesPerChannel, factory);
		
		
		Thread th = new Thread(transmitterThread);
		th.start();
		try{
			final AtomicBoolean messageSent = new AtomicBoolean(false);
			
			/**
			 * GRingSnapshot gring,
				MessageReceiptListener messageReceiptListener,
				MessageTransmitListener messageTransmitListener
			 */
			
			//now add a message to the queue
			Message message = new MessageImpl(1L, Message.TYPE.WRITE, new GRingSnapshotImpl(null), null, new MessageTransmitListener(){
	
				@Override
				public void messageSent(Message request) {
					messageSent.set(true);
				}
	
				@Override
				public void error(Message request, Throwable t) {
					t.printStackTrace();
					messageSent.set(false);
				}
				
			});
			
			
			messageQueue.add(message);
			
			Thread.sleep(500L);
			
			assertTrue(messageSent.get());
		}finally{
			th.interrupt();
		}
	}


	private GRingClientFactory createFactory(final boolean errorOnTransmit){
		return new GRingClientFactory() {
			
			
			@Override
			public GRingClient create() {
				return new GRingClient() {
					
					@Override
					public void transmit(Message request) {
						if(errorOnTransmit){
							throw new RuntimeException("Induced Error");
						}
					}
					
					@Override
					public void open(InetAddress inetAddress) {
						
					}
					
					@Override
					public boolean isClosed() {
						return false;
					}
					
					@Override
					public void close() {
						
					}
				};
			}
		};
	}
	
	@Override
	protected void setUp() throws Exception {
		
		
	}
	
	
	
	
	
}
