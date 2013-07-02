package org.streams.commons.file;

import java.util.concurrent.TimeUnit;

/**
 * 
 * An abstract of idea of coordination sending bytes from an agent to a cluster
 * of collectors.<br/>
 * The collectors need to communicate between each other on the current file
 * pointer sent by the agent.<br/>
 * This allows the collectors to know if an agent is sending a duplicate or have
 * missed bytes.<br/>
 */
public interface CoordinationServiceClient {

	/**
	 * Clean any external resources
	 */
	void destroy();

	<T> T withLock(FileStatus.FileTrackingStatus fileStatus, long timeout,
			TimeUnit unit,
			CoordinationServiceListener<T> coordinationServiceListener)
			throws Exception;

	public static interface CoordinationServiceListener<T> {

		T inSync(FileStatus.FileTrackingStatus file, SyncPointer pointer,
				PostWriteAction writeAction) throws Exception;

		T syncConflict(FileStatus.FileTrackingStatus file, SyncPointer pointer)
				throws Exception;

	}

}
