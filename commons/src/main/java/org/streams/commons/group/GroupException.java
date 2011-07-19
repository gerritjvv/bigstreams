package org.streams.commons.group;

/**
 * 
 * All Group Exceptions are encapsulated in a RuntimeException
 *
 */
public class GroupException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public GroupException() {
		super();
	}

	public GroupException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public GroupException(String arg0) {
		super(arg0);
	}

	public GroupException(Throwable arg0) {
		super(arg0);
	}

	
	
}
