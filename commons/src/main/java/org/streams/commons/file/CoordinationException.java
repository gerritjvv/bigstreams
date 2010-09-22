package org.streams.commons.file;

public class CoordinationException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	int code = 0;
	
	public CoordinationException() {
		super();
	}


	public CoordinationException(int code, String message, Throwable cause) {
		super(message, cause);
		this.code = code;
	}
	
	public CoordinationException(String message, Throwable cause) {
		super(message, cause);
	}

	public CoordinationException(String message) {
		super(message);
	}

	public CoordinationException(Throwable cause) {
		super(cause);
	}


	public int getCode() {
		return code;
	}


	public void setCode(int code) {
		this.code = code;
	}

	
}
