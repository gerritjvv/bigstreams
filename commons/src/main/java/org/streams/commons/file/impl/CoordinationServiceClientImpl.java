package org.streams.commons.file.impl;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;
import org.streams.commons.file.CoordinationServiceClient;
import org.streams.commons.file.FileStatus;
import org.streams.commons.file.FileStatus.FileTrackingStatus;
import org.streams.commons.file.FileStatus.FileTrackingStatus.Builder;
import org.streams.commons.file.PostWriteAction;
import org.streams.commons.file.SyncPointer;
import org.streams.commons.zookeeper.ZLock;
import org.streams.commons.zookeeper.ZStore;

/**
 * 
 * 
 * This class is thread safe.<br/>
 * Two ExecutorService(s) and a HashedWheelTimer are created when the object is
 * instantiated.<br/>
 * These instances are reused through all client connections with the server.<br/>
 * To shutdown and close these resources call the destroy method.<br/>
 * 
 */
public class CoordinationServiceClientImpl implements CoordinationServiceClient {

	ZLock zlock;
    ZStore zstore;
	
	public CoordinationServiceClientImpl(ZLock zlock, ZStore zstore) {
		super();
		this.zlock = zlock;
		this.zstore = zstore;
	}

	@Override
	public void destroy() {

	}

	/**
	 * (1)WithLock for logType agentName fileName (2) Get SyncPointer for file
	 * (3) If fileStatus.pointer == SyncPointer.pointer (3).1 Call inSync, with
	 * PostWriteAction to save SyncPointer once written (4) Else (4).1 Call
	 * syncConflict
	 */
	@Override
	public void withLock(final FileStatus.FileTrackingStatus fileStatus,
			final CoordinationServiceListener coordinationServiceListener)
			throws Exception {

		final String lockId = fileStatus.getAgentName() + fileStatus.getLogType()
				+ fileStatus.getFileName().replace('/', '_');

		zlock.withLock(lockId, new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {

				final SyncPointer pointer = getSyncPointer(lockId, fileStatus);

				if (fileStatus.getFilePointer() == pointer.getFilePointer()) {

					// we call the inSync method, and PostWriteAction, the
					// listener should write out
					// the client data and then call the PostWriteAction
					coordinationServiceListener.inSync(fileStatus, pointer,
							new PostWriteAction() {

								@Override
								public void run(int bytesWritten)
										throws Exception {
									pointer.incFilePointer(bytesWritten);
									pointer.setLinePointer(fileStatus
											.getLinePointer());

									// save pointer -- on exception the output
									// streams is rolled back.
									// -- ensures that pointer save and file
									// output is atomic.
									saveSyncPointer(lockId, pointer, fileStatus);
								}

							});

				} else {
					coordinationServiceListener.syncConflict(fileStatus,
							pointer);
				}

				return null;
			}

		});

	}

	private void saveSyncPointer(String key, SyncPointer pointer, FileStatus.FileTrackingStatus fileStatus) throws IOException, InterruptedException, KeeperException {
		//update a new file status instance with the pointer data
		Builder builder = FileStatus.FileTrackingStatus.newBuilder(fileStatus);
		
		builder.setFilePointer(pointer.getFilePointer());
		builder.setLinePointer(pointer.getLinePointer());
		
		zstore.store(key, builder.build());
	}

	private final SyncPointer getSyncPointer(String key, FileStatus.FileTrackingStatus fileStatus) throws IOException, InterruptedException, KeeperException {
		FileStatus.FileTrackingStatus status = (FileTrackingStatus) zstore.get(key,  FileStatus.FileTrackingStatus.newBuilder());
		if(status == null){
			//no status was saved before
			status = fileStatus;
		}
			
		
		return new SyncPointer(status);
	}

	public ZLock getZlock() {
		return zlock;
	}

	public void setZlock(ZLock zlock) {
		this.zlock = zlock;
	}

}
