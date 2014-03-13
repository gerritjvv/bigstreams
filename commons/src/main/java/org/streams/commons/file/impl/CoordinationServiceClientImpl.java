package org.streams.commons.file.impl;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

	private static final Logger LOG = Logger
			.getLogger(CoordinationServiceClientImpl.class);

	final ZLock zlock;
	final ZStore zstore;

	final AtomicBoolean isAgentSync = new AtomicBoolean(true);

	public CoordinationServiceClientImpl(ZLock zlock, ZStore zstore) {
		super();
		this.zlock = zlock;
		this.zstore = zstore;
		isAgentSync.set(!(System.getenv("agent.sync") == null || System
				.getProperty("agent.sync") == null));

		if (isAgentSync.get()) {
			LOG.info("Agent is is on");

			new Thread() {
				public void run() {
					try {
						// 15 minutes
						Thread.sleep(900000);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return;
					} finally {
						isAgentSync.set(false);
						LOG.info("Turning agent sync off");
					}
				}
			}.start();
		}
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
	public final <T> T withLock(final FileStatus.FileTrackingStatus fileStatus,
			long timeout, TimeUnit unit,
			final CoordinationServiceListener<T> coordinationServiceListener)
			throws Exception {

		final String lockId = fileStatus.getAgentName()
				+ fileStatus.getLogType()
				+ fileStatus.getFileName().replace('/', '_');

		return zlock.withLock(lockId, timeout, unit, new Callable<T>() {

			@Override
			public T call() throws Exception {

				final SyncPointer pointer = getSyncPointer(lockId, fileStatus);

				if (fileStatus.getFilePointer() == pointer.getFilePointer()) {

					// we call the inSync method, and PostWriteAction, the
					// listener should write out
					// the client data and then call the PostWriteAction
					return coordinationServiceListener.inSync(fileStatus,
							pointer, new PostWriteAction() {

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
					if (isAgentSync.get()) {
						// we call the inSync method, and PostWriteAction, the
						// listener should write out
						// the client data and then call the PostWriteAction
						LOG.info("Collector is adjusting file pointer to agent "
								+ fileStatus.getAgentName()
								+ " file pointer "
								+ fileStatus.getFilePointer()
								+ " for file "
								+ fileStatus.getFileName());

						return coordinationServiceListener.inSync(fileStatus,
								pointer, new PostWriteAction() {

									@Override
									public void run(int bytesWritten)
											throws Exception {
										pointer.setFilePointer(fileStatus
												.getFilePointer());
										pointer.incFilePointer(bytesWritten);
										pointer.setLinePointer(fileStatus
												.getLinePointer());

										// save pointer -- on exception the
										// output
										// streams is rolled back.
										// -- ensures that pointer save and file
										// output is atomic.
										saveSyncPointer(lockId, pointer,
												fileStatus);
									}

								});

					} else {
						return coordinationServiceListener.syncConflict(
								fileStatus, pointer);
					}
				}

			}

		});

	}

	private final void saveSyncPointer(String key, SyncPointer pointer,
			FileStatus.FileTrackingStatus fileStatus) throws IOException,
			InterruptedException, KeeperException {
		// update a new file status instance with the pointer data
		Builder builder = FileStatus.FileTrackingStatus.newBuilder(fileStatus);

		builder.setFilePointer(pointer.getFilePointer());
		builder.setLinePointer(pointer.getLinePointer());
		FileTrackingStatus statusNew = builder.build();

		zstore.store(key, statusNew);
	}

	private final SyncPointer getSyncPointer(String key,
			FileStatus.FileTrackingStatus fileStatus) throws Exception {
		FileStatus.FileTrackingStatus status = (FileTrackingStatus) zstore.get(
				key, FileStatus.FileTrackingStatus.newBuilder());

		SyncPointer syncPointer;

		if (status == null) {
			// no status was saved before
			status = fileStatus;
			syncPointer = new SyncPointer(status);
		} else {

			if (fileStatus.getFilePointer() != status.getFilePointer()) {

				//might be out of sync sync client
				zstore.sync(key);
				status = (FileTrackingStatus) zstore.get(key,
						FileStatus.FileTrackingStatus.newBuilder());
				
				//check again
				if (fileStatus.getFilePointer() != status.getFilePointer()) {

					if (status != null) {
						syncPointer = new SyncPointer(status);

						LOG.info("Possible sync conflict: syncing collector with zookeeper: "
								+ fileStatus.getFilePointer()
								+ " zookeeper.old "
								+ fileStatus.getFilePointer()
								+ " zookeeper.new "
								+ status.getFilePointer()
								+ " key: "
								+ key
								+ " syncid: "
								+ syncPointer.getTimeStamp());

					}
				} else {
					// check null again
					if (status == null) {
						status = fileStatus;
					}

					syncPointer = new SyncPointer(status);
				}
			}

			// check null again
			if (status == null) {
				status = fileStatus;
			}

			syncPointer = new SyncPointer(status);

		}

		return syncPointer;
	}

}
