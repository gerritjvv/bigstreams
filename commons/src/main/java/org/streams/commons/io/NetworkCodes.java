package org.streams.commons.io;

/**
 * 
 * Define the network response codes used by the agent and the collector
 *
 */
public class NetworkCodes {

	public static enum CODE { 
		UNKOWN(500, "Unkown error"),
		COORDINATION_CONNECTION_ERROR(5600, "Coordination connection error"),
		COORDINATION_LOCK_ERROR(5601, "Error obtaining lock from coordination service"),
		COORDINATION_UNLOCK_ERROR(5602, "Error unlocking from coordination service"),
		SYNC_CONFLICT(409, "Agent is out of sync with coordination service");
		
	   int num = 0;
	   String msg = null;
	   
	   CODE(int num, String msg){
		   this.num = num;
		   this.msg = msg;
	   }
	   
	   public int num(){return num;}
	   public String msg() { return msg; }
	   
	   public String toString(){
		   return String.valueOf(num);
	   }
	   
	   
	}
	
	public static CODE findCode(int code){
		for(CODE  ecode : CODE.values()){
			if(ecode.num() == code){
				return ecode;
			}
		}
		
		return CODE.UNKOWN;
	}
	
}
