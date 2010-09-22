package org.streams.agent.send;

/**
 * Thrown if the client send method generates any error 
 *
 */
public class ClientException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * The client error code
	 */
	int code = 0;

	public ClientException(String message, int code) {
		super(message);
	}

	public ClientException(String message, Throwable cause, int code) {
		super(message, cause);
	}
	
	public String toString(){
		return super.toString() + " code: " + code;
	}

}
