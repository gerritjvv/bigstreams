package org.streams.agent.send;

/**
 * Thrown if the Server returned an error code i.e. a value other than 200
 */
public class ServerException extends RuntimeException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * The code that the server returns to the client
	 */
	int code = 0;
	
	public ServerException(String msg, int code){
		super(msg);
	}
	public ServerException(String msg, Throwable t, int code){
		super(msg, t);
	}
	
	public String toString(){
		return super.toString() + "; error_code: " + code;
	}
	
}
