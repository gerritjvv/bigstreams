package org.streams.gring.group.error;

/**
 * Thrown when no members in the GRing can be found.
 * 
 */
public class NoGRingMemebersException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NoGRingMemebersException() {
		super();
	}

	public NoGRingMemebersException(String message, Throwable cause) {
		super(message, cause);
	}

	public NoGRingMemebersException(String message) {
		super(message);
	}

	public NoGRingMemebersException(Throwable cause) {
		super(cause);
	}

}
