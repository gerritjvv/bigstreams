package org.streams.gring.transmit.impl;

import java.util.SortedSet;
import java.util.concurrent.BlockingDeque;

import org.apache.log4j.Logger;
import org.streams.gring.group.GRing;
import org.streams.gring.group.MemberDesc;
import org.streams.gring.group.error.GRingMemberComException;
import org.streams.gring.group.error.NoGRingMemebersException;
import org.streams.gring.message.Message;
import org.streams.gring.net.GRingClient;
import org.streams.gring.net.GRingClientFactory;

/**
 * 
 * Each instance of the Transmitter Thread will poll the Message Queue for
 * messages. Flow is:<br/>
 * <ul>
 * <li>Get Message m from queue.</li>
 * <li>open client if not open</li>
 * <li>send message</li>
 * <li>if messages sent counter >= messagesPerChannel close client and create
 * new client.</li>
 * <li>loop</li>
 * </ul>
 * 
 */
public class TransmitterThread implements Runnable {

	private static final Logger LOG = Logger.getLogger(TransmitterThread.class);
	
	BlockingDeque<Message> messageQueue;
	int messagesPerChannel;
	
	int currentMessageCount = 0;
	
	GRingClientFactory gringClientFactory;
	GRing gring;
	
	GRingClient currentClient;
	
	
	/**
	 * 
	 * @param gring  Shared between all instances of TransmitterThread to know the group ring members.
	 * @param messageQueue Shares between all instances of TransmitterThread to get new messages to transmit.
	 * @param messagesPerChannel Multiple messages can be transmitted over the same channel, this increases throughput.
	 * @param gringClientFactory Create instances of GRingClient.
	 */
	public TransmitterThread(GRing gring, BlockingDeque<Message> messageQueue,
			int messagesPerChannel, GRingClientFactory gringClientFactory) {
		super();
		this.gring = gring;
		this.messageQueue = messageQueue;
		this.messagesPerChannel = messagesPerChannel;
		this.gringClientFactory = gringClientFactory;
	}




	/**
	 * Send a message trying each Member in turn when a send fails to any one member.<br/>
	 * Flow is:<br/>
	 * <ul> 
	 *  <li> Get members set </li>
	 *  <li> if members set is empty send NoGRingMemebersException the MessageTransmitListener.</li>
	 *  <li> else: for each MemberDesc in member set:</li>
	 *  <li> open client connection if not open and transmit message.<li>
	 *  <li> On Error: the run method will catch any errors, black list the node, re discover the network, and send again.</li>
	 * </ul>
	 * @param message
	 */
	private void sendMessage(Message message){
		
		SortedSet<MemberDesc> members = gring.getMembers();
		
		if(members == null || members.isEmpty()){
			message.getMessageTransmitListener().error(message, new NoGRingMemebersException("No GRing Members are present"));
		}else{
			
			//get the successor. :-- Messages are always sent to the successor.
			MemberDesc successor = members.first();
			
		
			try{
				//open a new client connection only if the currentClient instance is null
				if(currentClient == null){
					currentClient = gringClientFactory.create();
					currentClient.open(successor.getInetAddress());
					currentMessageCount = 0;
				}
						
				currentClient.transmit(message);
				currentMessageCount++;
				
				message.getMessageTransmitListener().messageSent(message);
				
				//close the client if the messagesPerChannel has been reched,
				//or if the currentClient has been closed by the server.
				if(currentMessageCount >= messagesPerChannel || currentClient.isClosed()){
					currentClient.close();
					currentClient = null;
				}

			}catch(Throwable t){
				currentClient.close();
				currentClient = null;
				
				LOG.error("Error transmitting message " + message + " to " + successor, t);
				throw new GRingMemberComException(successor);
			}
			
		
		}
		
		
	}
	
	
	
	
	public void run() {

		boolean interrupted = false;
		
		try {
			while (!(interrupted = Thread.interrupted())) {
				Message message = messageQueue.take();
				
				try{
					sendMessage( message );
				}catch(GRingMemberComException memberExcp){
					//blacklist and re-discover the network.
					gring.blackListMember(memberExcp.getMember());
					sendMessage(message);
				}
				
			}

		} catch (InterruptedException e) {
			interrupted = true;
		}

		if (interrupted) {
			Thread.currentThread().interrupt();
		}

	}

	
	
}
