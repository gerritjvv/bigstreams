package org.streams.commons.file.impl;

import java.util.concurrent.Callable;

import org.strams.commons.file.FileStatus;
import org.streams.commons.file.CoordinationServiceClient;
import org.streams.commons.file.PostWriteAction;
import org.streams.commons.file.SyncPointer;
import org.streams.commons.zookeeper.ZLock;

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

	public CoordinationServiceClientImpl(ZLock zlock) {
		super();
		this.zlock = zlock;
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

		String lockId = fileStatus.getLogType() + fileStatus.getAgentName()
				+ fileStatus.getFileName();

		zlock.withLock(lockId, new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {

				final SyncPointer pointer = getSyncPointer(fileStatus);

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
									saveSyncPointer(pointer);
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

	private void saveSyncPointer(SyncPointer pointer) {

	}

	private final SyncPointer getSyncPointer(
			FileStatus.FileTrackingStatus fileStatus) {

		return null;
	}

	public ZLock getZlock() {
		return zlock;
	}

	public void setZlock(ZLock zlock) {
		this.zlock = zlock;
	}

}
