package org.streams.collector.write.impl;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.log4j.Logger;
import org.streams.collector.mon.CollectorStatus;
import org.streams.collector.write.FileOutputStreamPool;
import org.streams.collector.write.LogRollover;
import org.streams.collector.write.LogRolloverCheck;
import org.streams.collector.write.RollBackOutputStream;
import org.streams.commons.compression.CompressionPool;
import org.streams.commons.compression.CompressionPoolFactory;
import org.streams.commons.util.concurrent.KeyLock;

/**
 * This is a utility class for managing multiple open file output streams.<br/>
 * The output streams are managed based on a key. The problem that this class
 * solves is<br/>
 * that the number of open files handlers is constraint and depends on the OS
 * settings.<br/>
 * <br/>
 * This class is Thread safe as per instance of the FileOutputStreamPool
 * <p/>
 * <b>Compression support</b><br/>
 * 
 * The problem with writing to a compression stream is that it can only be
 * opened once and then closed, but <br/>
 * normally not appended to. Although some formats support appending this is not
 * true for all.<br/>
 * This class allows the OutputStream to be opened as Compressed and then kept
 * open until some future operation calls the close methods.
 * 
 * 
 */
public class FileOutputStreamPoolImpl implements FileOutputStreamPool {

	private static final Logger LOG = Logger
			.getLogger(FileOutputStreamPoolImpl.class);

	/**
	 * Used to lock individual operations on a key
	 */
	final Map<String, RollBackOutputStream> fileHandleMap = new ConcurrentHashMap<String, RollBackOutputStream>();
	final Map<String, File> openFiles = new ConcurrentHashMap<String, File>();

	/**
	 * each tmie a file output stream is created an entry is added here.
	 */
	final Map<File, Long> fileCreationTimes = new ConcurrentHashMap<File, Long>();

	/**
	 * each time a open stream is requested and a file is released this map is
	 * updated.
	 */
	final Map<File, Long> fileUpdateTimes = new ConcurrentHashMap<File, Long>();

	KeyLock keyLock = new KeyLock();

	public static final long defaultOpenFileLimit = 30000L;

	LogRollover logRollover = null;

	/**
	 * The number of files that may be open at any time
	 */
	AtomicLong openFileLimit = new AtomicLong(defaultOpenFileLimit);

	int waitForRolloverLimit = 100;
	long acquireLockTimeout = 10 * 1000;

	CollectorStatus collectorStatus;

	CompressionPoolFactory compressionPoolFactory;

	/**
	 * Time that this class will wait for a compression resource to become
	 * available. Default 10000L
	 */
	private long waitForCompressionResource = 500L;

	public FileOutputStreamPoolImpl(LogRollover logRollover,
			CollectorStatus collectorStatus,
			CompressionPoolFactory compressionPoolFactory) {
		this.logRollover = logRollover;
		this.collectorStatus = collectorStatus;
		if (openFileLimit.get() == 0)
			openFileLimit.set(defaultOpenFileLimit);
		this.compressionPoolFactory = compressionPoolFactory;
	}

	public FileOutputStreamPoolImpl(LogRollover logRollover,
			long acquireLockTimeout, long openFileLimit,
			CollectorStatus collectorStatus,
			CompressionPoolFactory compressionPoolFactory) {
		this.logRollover = logRollover;
		this.acquireLockTimeout = acquireLockTimeout;
		this.openFileLimit.set(openFileLimit);
		this.collectorStatus = collectorStatus;
		this.compressionPoolFactory = compressionPoolFactory;
	}

	/**
	 * Will always return an opened output stream.<br/>
	 * 
	 * @param key
	 *            String
	 * @param ompressionCodec
	 *            if null no compression is used
	 * @param file
	 *            File the file will be created if it does not exist
	 * @param append
	 * @return
	 */
	public RollBackOutputStream open(String key,
			CompressionCodec compressionCodec, File file, boolean append)
			throws IOException {

		// if interrupted the exception will be thrown and nothing will be done
		// this lock is kept until the file has been released.
		// note that no rollover will be done if this lock is not released
		// i.e. the rollover will block on this lock untill the thread that
		// holds it releases it
		try {
			keyLock.acquireLock(key);
			return getOutputStream(key, file, compressionCodec);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return null;
		} catch (IOException e) {
			keyLock.releaseLock(key);
			throw e;
		} catch (Throwable t) {
			keyLock.releaseLock(key);
			RuntimeException rte = new RuntimeException(t);
			rte.setStackTrace(t.getStackTrace());
			throw rte;
		}

	}

	/**
	 * Returns the output stream for the key. If the file exists already it will
	 * be rolled over.<br/>
	 * If the file cannot be rolled here an exception will be thrown. <br/>
	 * This method assumes that the current thread already has a lock.
	 * 
	 * @param key
	 * @param file
	 * @param compressionCodec
	 * @return
	 * @throws IOException
	 */
	private RollBackOutputStream getOutputStream(String key, File file,
			CompressionCodec compressionCodec) throws IOException {

		RollBackOutputStream out = fileHandleMap.get(key);

		if (out == null) {
			if (fileHandleMap.size() >= openFileLimit.get()) {
				throw new IOException("Open File Limit reached limit is "
						+ openFileLimit.get());
			}

			// these operations need to happend atomically so we must synch
			if (file.length() > 0 && file.exists()) {
				// if the file already exists it must be rolled before
				closeLockedFile(key, file);

				if (file.exists()) {
					// we never expect this condition to happen but the local OS
					// might be out of file handlers causing this condition to
					// be
					// true
					throw new IOException(
							"The file "
									+ file.getAbsolutePath()
									+ " was rolled over but the OS failed to move the file");
				}

			}

			FileUtils.forceMkdir(file.getParentFile());
			file.createNewFile();
			
			// wait for file creation. This is needed if the file is stored
			// via NFS or other.
			// a 30 second limit is created
			boolean fileCreated = false;
			try {
				fileCreated = FileUtils.waitFor(file, 30);
			} catch (NullPointerException npe) {
				throw new IOException(npe.toString(), npe);
			}

			// file must be created here
			if (!fileCreated)
				throw new IOException("Failed to create file " + file);

			fileCreationTimes.put(file,
					Long.valueOf(System.currentTimeMillis()));

			openFiles.put(key, file);

			// if the compressionCodec is null use the FileOutputStream else
			// use the CompressionOutputStream
			// out = (compressionCodec == null) ? fout :
			// compressionCodec.createOutputStream(fout);
			// if compression is used append cannot be used:
			try {
				if (compressionCodec == null) {
					out = new RollBackOutputStream(file,
							new TextFileStreamCreator(acquireLockTimeout), 0L);
				} else {

					// get a compressor resource
					CompressionPool pool = compressionPoolFactory
							.get(compressionCodec);

					// remember the pool that locked the resource for the
					// CompressionOutputStream
					// this resource will be released when this class
					// (FileOutputStreamPoolImpl) closes the
					// RollBackOutputStream
					/**
					 * File file, StreamCreator streamCreator, long initialSize
					 */
					out = new RollBackOutputStream(file,
							new CompressedStreamCreator(compressionCodec, pool,
									waitForCompressionResource,
									acquireLockTimeout), 0L);
				}

			} catch (InterruptedException e) {
				Thread.interrupted();
				throw new IOException("No compression resource available for "
						+ compressionCodec);
			}

			fileHandleMap.put(key, out); // save the output stream

			collectorStatus.incCounter(
					CollectorStatus.COUNTERS.FILES_OPEN.toString(), 1);

		}

		fileUpdateTimes.put(file, System.currentTimeMillis());

		return out;
	}

	/**
	 * This method will try to rollover files emdiatly if they are not being
	 * locked by another thread.<br/>
	 * All locks that are still in the lock map but do not currently have a lock
	 * will also be removed, cleaning out the lock file map.<br/>
	 * This method is meant to be called by a separate thread.
	 */
	public void checkFilesForRollover(LogRolloverCheck rolloverCheck)
			throws IOException {

		Set<String> keys = openFiles.keySet();

		if(keys == null){
			return;
		}
		
		// we're using concurrent hashmap so the set is safe
		// for each key we will:
		// --- (1) - check to see if we should roll it over
		// --- (2) - if so try to get a lock on it.
		// --- (2.a) - If no lock can be acquired in 100ms we will not roll this
		// file.
		// --- (2.b) - If a lock can be acquired we will roll this file and
		// release the lock.
		for (String key : keys) {

			File file = openFiles.get(key);

			if (file == null) {
				continue;
			}

			// the time in milliseconds when the file was created
			Long creationTime = fileCreationTimes.get(file);
			// the time in milliseconds when the file was last updated
			Long updateTime = fileUpdateTimes.get(file);

			boolean shouldRollover = false;
			try{
			 shouldRollover = rolloverCheck.shouldRollover(file,
					creationTime, updateTime);
			}catch(Throwable t){
				t.printStackTrace();
				LOG.error("rolloverCheck: " + rolloverCheck + " file : " + file + " error: " + t.toString(), t);
			}
			
			if (shouldRollover) {
				boolean lockAcquired;

				try {
					// this try lock is still good for inactivity checks also
					// because an inactive file will not have any locks.
					lockAcquired = keyLock.acquireLock(key, 1000L);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					// we need to thread to leave this method immediately
					return;
				}

				if (lockAcquired) {
					try {
						closeLockedFile(key, openFiles.get(key));
					} finally {
						keyLock.releaseLock(key);
					}
				} else {
					LOG.warn("Failed to acquire a lock on "
							+ file.getAbsolutePath()
							+ " this file will be rolled over later");
				}

			}

		}
	}

	/**
	 * This method will remove an entry from the fileWriteMap indicating that
	 * this file is not written to anymore.<br/>
	 * Then if any entries for this key exist in the rolloverNotifyMap the file
	 * will be closed and the file rolled.
	 * 
	 * @param key
	 * @throws IOException
	 */
	public void releaseFile(String key) throws IOException {

		keyLock.releaseLock(key);

		File file = openFiles.get(key);
		if (file != null)
			fileUpdateTimes.put(file, System.currentTimeMillis());

	}

	/**
	 * Closes all OutputStreams kept opened by this class
	 * 
	 * @throws IOException
	 */
	public void closeAll() throws IOException {
		while (fileHandleMap.keySet().size() > 0) {
			close(fileHandleMap.keySet().iterator().next());
		}
	}

	/**
	 * This is for internal closes where the key has already been locked and the
	 * close operation can operate without fear of another process accessing
	 * this key's file.<br/>
	 * 
	 * @param key
	 * @throws IOException
	 */
	private void closeLockedFile(String key, File file) throws IOException {
		// remove from openFiles
		File fileFound = openFiles.remove(key);

		if (file == null) {
			LOG.info("No file found to roll for " + key);
			return;
		}

		if (fileFound == null) {
			LOG.info("The file "
					+ file.getAbsolutePath()
					+ " was not rolled previousely before shutdown. This file will be rolled now");
		}

		if (file != null) {
			fileCreationTimes.remove(file);

			RollBackOutputStream out = fileHandleMap.remove(key);

			// locks must be released before closing files

			if (out != null) {

				try {
					// If Compression is used:
					// this method will also call the the
					// CompressedStreamCreator.close that will in turn call the
					// CompressionPool closeAndRelease method
					// releasing the Compressor resource.
					out.close();
				} catch (IOException e) {
					// we close quitely here.
					LOG.error(e.toString(), e);
				}
			}

			// on closing a file should be automatically rolled over.
			// rollover means that the file will be available by other
			// processes which only check for rolled files.
			// if we don't roll here and the collect does not get started
			// for some time the un rolled file will stay
			// unprocessed by this processed

			File rolledOverFile = logRollover.rollover(file);

			// we should check the the LogRollover works as expected
			if (rolledOverFile == null
					|| rolledOverFile.getAbsolutePath().equals(
							file.getAbsolutePath())) {
				throw new IOException("The LogRollover " + logRollover
						+ " did not rollover the file " + file.getName()
						+ " a expected");
			}

			LOG.debug("notifyLogRollover: Rolling over file " + key + " to "
					+ rolledOverFile.getName());

			collectorStatus.decCounter(
					CollectorStatus.COUNTERS.FILES_OPEN.toString(), 1);
		}

	}

	/**
	 * Will close an output stream and remove it from the fileHandleMap.<br/>
	 * 
	 * @param key
	 * @throws IOException
	 */
	public void close(String key) throws IOException {

		try {
			keyLock.acquireLock(key);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}

		try {

			closeLockedFile(key, openFiles.get(key));

		} finally {
			keyLock.releaseLock(key);
		}

	}

	@Override
	public RollBackOutputStream open(String key, File file, boolean append)
			throws IOException {
		return open(key, null, file, append);
	}

	public CompressionPoolFactory getCompressionPoolFactory() {
		return compressionPoolFactory;
	}

	public void setCompressionPoolFactory(
			CompressionPoolFactory compressionPoolFactory) {
		this.compressionPoolFactory = compressionPoolFactory;
	}

	public long getWaitForCompressionResource() {
		return waitForCompressionResource;
	}

	public void setWaitForCompressionResource(long waitForCompressionResource) {
		this.waitForCompressionResource = waitForCompressionResource;
	}

}
