package org.streams.commons.file;

import org.strams.commons.file.FileStatus;


/**
 * 
 * An abstract of idea of coordination sending bytes from an agent to a cluster of collectors.<br/>
 * The collectors need to communicate between each other on the current file pointer sent by the agent.<br/>
 * This allows the collectors to know if an agent is sending a duplicate or have missed bytes.<br/>
 */
public interface CoordinationServiceClient {

	
	/**
	 * Clean any external resources
	 */
	void destroy();
	
	void withLock(FileStatus.FileTrackingStatus fileStatus,
			CoordinationServiceListener coordinationServiceListener) throws Exception;
	
	public static interface CoordinationServiceListener{

		void inSync(FileStatus.FileTrackingStatus file, SyncPointer pointer, PostWriteAction writeAction) throws Exception;

		void syncConflict(FileStatus.FileTrackingStatus file, SyncPointer pointer) throws Exception;
		
		
	}


	
	
}
