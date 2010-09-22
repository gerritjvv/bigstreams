package org.streams.agent.file;

/**
 * 
 * Encapsulates the information of last line number read and file pointer in
 * bytes
 * 
 */
public class FileLinePointer implements Cloneable{

	long filePointer = 0;
	int linePointer = 0;

	//used to indicate that a conflict with the server occurred
	//the conflictFilePointer is the pointer that the server has
	long conflictFilePointer = -1L;
	
	public FileLinePointer() {
	}

	public FileLinePointer(long filePointer, int linePointer) {
		super();
		this.filePointer = filePointer;
		this.linePointer = linePointer;
	}

	public long getFilePointer() {
		return filePointer;
	}

	public void incFilePointer(int filePointerInc) {
		filePointer += filePointerInc;
	}
	
	public void setFilePointer(long filePointer) {
		this.filePointer = filePointer;
	}

	public int getLineReadPointer() {
		return linePointer;
	}

	public void setLineReadPointer(int lineRead) {
		this.linePointer = lineRead;
	}

	public void incLineReadPointer(int lineReadInc) {
		linePointer += lineReadInc;
	}

	/**
	 * Copies the values of the argument passed to the internal state of the current FileLinePointer incrementing the current state.
	 * @param fileLinePointer
	 */
	public void copyIncrement(FileLinePointer fileLinePointer){
		filePointer += fileLinePointer.getFilePointer();
		linePointer += fileLinePointer.getLineReadPointer();
		conflictFilePointer = fileLinePointer.getConflictFilePointer();
	}
	
	
	/**
	 * Copies the values of the argument passed to the internal state of the current FileLinePointer.
	 * @param fileLinePointer
	 */
	public void copyIn(FileLinePointer fileLinePointer){
		filePointer = fileLinePointer.getFilePointer();
		linePointer = fileLinePointer.getLineReadPointer();
		conflictFilePointer = fileLinePointer.getConflictFilePointer();
	}
	
	/**
	 * Creates a copy of the current state of the FileLinePointer
	 */
	@Override
	public Object clone(){
		return new FileLinePointer(filePointer, linePointer);
	}

	public boolean hasConflictFilePointer(){
		return conflictFilePointer != -1;
	}
	public long getConflictFilePointer() {
		return conflictFilePointer;
	}

	public void setConflictFilePointer(long conflictFilePointer) {
		this.conflictFilePointer = conflictFilePointer;
	}

}
