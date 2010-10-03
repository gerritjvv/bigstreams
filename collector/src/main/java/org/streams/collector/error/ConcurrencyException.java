package org.streams.collector.error;

/**
 * Signals a concurrency exception
 * 
 */
public class ConcurrencyException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ConcurrencyException() {
		super();
	}

	public ConcurrencyException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConcurrencyException(String message) {
		super(message);
	}

	public ConcurrencyException(Throwable cause) {
		super(cause);
	}

}
