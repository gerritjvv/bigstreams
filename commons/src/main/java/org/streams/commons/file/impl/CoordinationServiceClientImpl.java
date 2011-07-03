package org.streams.commons.file.impl;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
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

	private static final Logger LOG = Logger.getLogger(CoordinationServiceClientImpl.class);
	
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
	public <T> T withLock(final FileStatus.FileTrackingStatus fileStatus,
			final CoordinationServiceListener<T> coordinationServiceListener)
			throws Exception {

		final String lockId = fileStatus.getAgentName() + fileStatus.getLogType()
				+ fileStatus.getFileName().replace('/', '_');

		return zlock.withLock(lockId, new Callable<T>() {

			@Override
			public T call() throws Exception {

				final SyncPointer pointer = getSyncPointer(lockId, fileStatus);
				
				
				if (fileStatus.getFilePointer() == pointer.getFilePointer()) {

					// we call the inSync method, and PostWriteAction, the
					// listener should write out
					// the client data and then call the PostWriteAction
					return coordinationServiceListener.inSync(fileStatus, pointer,
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
					return coordinationServiceListener.syncConflict(fileStatus,
							pointer);
				}

			}

		});

	}

	private void saveSyncPointer(String key, SyncPointer pointer, FileStatus.FileTrackingStatus fileStatus) throws IOException, InterruptedException, KeeperException {
		//update a new file status instance with the pointer data
		Builder builder = FileStatus.FileTrackingStatus.newBuilder(fileStatus);
		
		builder.setFilePointer(pointer.getFilePointer());
		builder.setLinePointer(pointer.getLinePointer());
		FileTrackingStatus statusNew = builder.build();
		
		zstore.store(key, statusNew);
	}

	private final SyncPointer getSyncPointer(String key, FileStatus.FileTrackingStatus fileStatus) throws IOException, InterruptedException, KeeperException {
		FileStatus.FileTrackingStatus status = (FileTrackingStatus) zstore.get(key,  FileStatus.FileTrackingStatus.newBuilder());
		
		SyncPointer syncPointer;
		
		if(status == null){
			//no status was saved before
			status = fileStatus;
			syncPointer = new SyncPointer(status);
		}else{
			
			if(fileStatus.getFilePointer() != status.getFilePointer()){
				//the zookeeper client might be out of sync. sync and retry.
				
				long statusFilePointer = status.getFilePointer();
				
				zstore.sync(key);
				status = (FileTrackingStatus) zstore.get(key,  FileStatus.FileTrackingStatus.newBuilder());
		
				
				if(status != null){
					syncPointer = new SyncPointer(status);
					
					LOG.info("Possible sync conflict: syncing zookeeper agent: " + fileStatus.getFilePointer()
							+ " zookeeper.old " + statusFilePointer 
							+ " zookeeper.new " + status.getFilePointer() + " key: " + key + " syncid: " + syncPointer.getTimeStamp());
					
				}
			}
			
			//check null again
			if(status == null){
				status = fileStatus;
			}
			
			syncPointer = new SyncPointer(status);
			
		}
		
		
		return syncPointer;
	}

	public ZLock getZlock() {
		return zlock;
	}

	public void setZlock(ZLock zlock) {
		this.zlock = zlock;
	}

}
