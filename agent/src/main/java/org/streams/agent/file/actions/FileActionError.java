package org.streams.agent.file.actions;

/**
 * 
 * All errors that happen in FileAction instances should be wrapped via this class.
 *
 */
public class FileActionError extends RuntimeException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public FileActionError() {
		super();
	}

	public FileActionError(String message, Throwable cause) {
		super(message, cause);
	}

	public FileActionError(String message) {
		super(message);
	}

	public FileActionError(Throwable cause) {
		super(cause);
	}

	
}
