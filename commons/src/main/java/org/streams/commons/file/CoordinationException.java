package org.streams.commons.file;

import java.net.ConnectException;

public class CoordinationException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	int code = 0;
	
	boolean isConnectException = false;
	
	public CoordinationException() {
		super();
	}

	
	public CoordinationException(int code, String message, Throwable cause) {
		super(message, cause);
		this.code = code;
		Throwable foundCause = cause;
		if(cause != null && cause instanceof CoordinationException){
			//find cause
			foundCause = cause.getCause();
		}
		isConnectException = foundCause != null && ConnectException.class.isAssignableFrom(foundCause.getClass());
		
	}
	
	public CoordinationException(String message, Throwable cause) {
		super(message, cause);
		Throwable foundCause = cause;
		if(cause != null && cause instanceof CoordinationException){
			//find cause
			foundCause = cause.getCause();
		}
		isConnectException = foundCause != null && ConnectException.class.isAssignableFrom(foundCause.getClass());
	}

	public CoordinationException(String message) {
		super(message);
	}

	public CoordinationException(Throwable cause) {
		super(cause);
		Throwable foundCause = cause;
		if(cause != null && cause instanceof CoordinationException){
			//find cause
			foundCause = cause.getCause();
		}
		isConnectException = foundCause != null && ConnectException.class.isAssignableFrom(foundCause.getClass());
	}


	public boolean isConnectException(){
		return isConnectException;
	}
	
	public int getCode() {
		return code;
	}


	public void setCode(int code) {
		this.code = code;
	}

	
}
