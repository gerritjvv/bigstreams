package org.streams.collector.write.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.log4j.Logger;
import org.streams.collector.write.FileOutputStreamPool;
import org.streams.collector.write.FileOutputStreamPoolFactory;
import org.streams.collector.write.LogFileNameExtractor;
import org.streams.collector.write.LogFileWriter;
import org.streams.collector.write.LogRolloverCheck;
import org.streams.commons.file.FileStatus;
import org.streams.commons.file.PostWriteAction;
import org.streams.commons.file.RollBackOutputStream;
import org.streams.commons.file.WriterException;

import com.google.common.io.CountingInputStream;

/**
 * This class has the following aims:<br/>
 * 
 * Split chunks out by logtype, and a date-time parameter. Currently the
 * date-time parameter is based on the original log file name sent from the
 * agent.
 * <p/>
 * <b>What type of log files</b> Really big ones :) <br/>
 * Everything about this class should be aimed at collecting huge log files and
 * will be optimized for this.<br/>
 * <p/>
 * <b>Why have this?:</b><br/>
 * A group of collectors already act as a pre hadoop cluster for log processing,
 * e.g. if we have 100 agents and <br/>
 * 5 collectors, we have a cluster of 5 machines that can do simple pre log
 * processing, rather than burden the hadoop ETL process<br/>
 * with this. Splitting log files up by log type and date does not require
 * looking at the actual data and is simple to do.<br/>
 * <p/>
 * <b></b> Several areas are of concern here:<br/>
 * <b>Disk space:</b><br/>
 * The writer should fail to write if there is not sufficient disk space (long
 * before filling up the entire disk).
 * <p/>
 * <b>Log Size</b><br/>
 * LZO of GZ should be used (Bzip2 can only write at most at 5mb/s so this might
 * be to slow (depending on disk speed).<br/>
 * LZO is the best here as the files can be loaded directly to HDFS.<br/>
 * Ideally log files should be at a standard 128mb or 256mb size. Its easier to
 * controll on the collector side than it is on hdfs.<br/>
 * <b>Log rotate</b> We want to open and close as less as possible files but at
 * the same time we want to make sure as soon as a Chunk has been AcK as OK its
 * persisted to disk. The rotate should be on ath 128mb or 256mb size or on some
 * time limit.<br/>
 * <p/>
 * This class is thread safe. i.e. all public methods are synchronized.
 * <p/>
 * <b>Thread safety<b/><br/>
 * This class will synchronise on a key value (log type date hour). This means
 * that two different keys will each write in parallel to different files, but
 * when the key is the same for two or more requests the code will synchronise
 * on the key value. This gives thread safety to the class in that no two
 * requests will ever write in parallel to the same file.<br/>
 * 
 */
public class LocalLogFileWriter implements LogFileWriter {

	private static final Logger LOG = Logger
			.getLogger(LocalLogFileWriter.class);

	File baseDir;
	CompressionCodec compressionCodec;
	LogRolloverCheck rolloverCheck;
	FileOutputStreamPoolFactory fileOutputStreamPoolFactory;

	Timer rolloverTimer = null;

	LogFileNameExtractor logFileNameExtractor;

	/**
	 * A timer is created and will check the log files for rollover.
	 */
	long logRolloverCheckPeriod = 1000L;

	/**
	 * This is only to be used for testing
	 */
	protected File lastWrittenFile;

	public File getBaseDir() {
		return baseDir;
	}

	public int write(FileStatus.FileTrackingStatus fileStatus, InputStream input)
			throws WriterException, InterruptedException {
		return write(fileStatus, input, null);
	}

	/**
	 * Helper function for writing
	 * 
	 * @param out
	 * @param v
	 * @throws IOException
	 */
	private static final void writeInt(final OutputStream out, final int v)
			throws IOException {
		out.write(0xff & (v >> 24));
		out.write(0xff & (v >> 16));
		out.write(0xff & (v >> 8));
		out.write(0xff & v);
	}

	/**
	 * 
	 * @return the number of bytes written
	 * @throws InterruptedException
	 */
	@Override
	public int write(FileStatus.FileTrackingStatus fileStatus,
			InputStream input, PostWriteAction postWriteAction)
			throws WriterException, InterruptedException {

		String key = logFileNameExtractor.getFileName(fileStatus);
		int wasWritten = 0;

		FileOutputStreamPool fileOutputStreamPool = fileOutputStreamPoolFactory
				.getPoolForKey(key);
		RollBackOutputStream outputStream = null;
		File file = null;
		try {
			file = getOutputFile(key);
			lastWrittenFile = file;

			outputStream = fileOutputStreamPool.open(key, compressionCodec,
					file, true);

			// we need to mark the current stream
			outputStream.mark();

			/**
			 * here we deserialise the base 64 data and write out
			 * [len][bts][len][bts]
			 */
			final CountingInputStream countInput = new CountingInputStream(
					input);
			final BufferedReader reader = new BufferedReader(new InputStreamReader(countInput));

			String line = null;
			try {
				while ((line = reader.readLine()) != null) {
					final byte[] bts = Base64.decodeBase64(line.getBytes("UTF-8"));

					writeInt(outputStream, bts.length);
					outputStream.write(bts);

				}
			} finally {
				wasWritten = (int)countInput.getCount();
				reader.close();
				countInput.close();
			}

			if (postWriteAction != null) {
				postWriteAction.run(wasWritten);
			}

		} catch (Throwable t) {

			LOG.error(t.toString(), t);
			if (outputStream != null && wasWritten > 0) {
				LOG.error("Rolling back file " + file.getAbsolutePath());
				// in case of any error we must roll back
				try {
					outputStream.rollback();
				} catch (IOException e) {
					throwException(e);
				} catch (InterruptedException e) {
					throw e;
				}
			}

			throwException(t);

		} finally {
			// make sure to releaseFile after writing
			// this line throws an IOException for this its embedded
			// inside
			// the
			// try catch.
			try {
				fileOutputStreamPool.releaseFile(key);
			} catch (IOException e) {
				throwException(e);
			}
		}

		return wasWritten;

	}

	/**
	 * Helper method to throw an exception with a correct stack trace.
	 */
	private static final void throwException(Throwable t)
			throws WriterException {
		WriterException writerException = new WriterException(t.toString(), t);
		writerException.setStackTrace(t.getStackTrace());
		throw writerException;
	}

	/**
	 * Get the file name based on the CompressionCodec.<br/>
	 * If a CompressionCodec is present the getDefaultExtension() method will be
	 * called and its return value used as the file extension.
	 * 
	 * @param key
	 * @return
	 */
	private final File getOutputFile(String key) {
		return (compressionCodec == null) ? new File(baseDir, key) : new File(
				baseDir, key + compressionCodec.getDefaultExtension());
	}

	/**
	 * Close all resources related with the LocalLogFileWriter
	 */
	public void close() throws WriterException {

		LOG.info("Closing files");

		rolloverTimer.cancel();
		rolloverTimer = null;

		fileOutputStreamPoolFactory.closeAll();

	}

	public void init() {
		rolloverTimer = new Timer("LocalFileWriter-LogRolloverCheck");

		// use schedule instead of fixed schedule
		rolloverTimer.schedule(new RolloverChecker(fileOutputStreamPoolFactory,
				rolloverCheck), 10000L, logRolloverCheckPeriod);

	}

	static class RolloverChecker extends TimerTask {

		final FileOutputStreamPoolFactory fileOutputStreamPoolFactory;
		final LogRolloverCheck logRolloverCheck;

		public RolloverChecker(
				FileOutputStreamPoolFactory fileOutputStreamPoolFactory,
				LogRolloverCheck logRolloverCheck) {
			this.fileOutputStreamPoolFactory = fileOutputStreamPoolFactory;
			this.logRolloverCheck = logRolloverCheck;

		}

		boolean canceled = false;

		public boolean cancel() {
			canceled = true;
			return super.cancel();
		}

		@Override
		public void run() {

			LOG.debug("Checking files for rollover notification");

			if (canceled) {
				LOG.error("Timer has been canceled");
				return;
			}

			try {
				// do not send if the rolloverCheck is null this might happen
				// due to threading and this method
				// being called from the Timer Thread.
				LOG.debug("Using rollover class: " + logRolloverCheck);

				if (logRolloverCheck != null
						&& fileOutputStreamPoolFactory != null)
					fileOutputStreamPoolFactory
							.checkFilesForRollover(logRolloverCheck);
			} catch (IOException e) {
				LOG.error(e.toString(), e);
			}

		}
	}

	public CompressionCodec getCompressionCodec() {
		return compressionCodec;
	}

	public void setCompressionCodec(CompressionCodec compressionCodec) {
		this.compressionCodec = compressionCodec;
	}

	public LogRolloverCheck getRolloverCheck() {
		return rolloverCheck;
	}

	public void setRolloverCheck(LogRolloverCheck rolloverCheck) {
		this.rolloverCheck = rolloverCheck;
	}

	public File getLastWrittenFile() {
		return lastWrittenFile;
	}

	public void setBaseDir(File baseDir) {
		this.baseDir = baseDir;
	}

	public LogFileNameExtractor getLogFileNameExtractor() {
		return logFileNameExtractor;
	}

	public void setLogFileNameExtractor(
			LogFileNameExtractor logFileNameExtractor) {
		this.logFileNameExtractor = logFileNameExtractor;
	}

	public FileOutputStreamPoolFactory getFileOutputStreamPoolFactory() {
		return fileOutputStreamPoolFactory;
	}

	public void setFileOutputStreamPoolFactory(
			FileOutputStreamPoolFactory fileOutputStreamPoolFactory) {
		this.fileOutputStreamPoolFactory = fileOutputStreamPoolFactory;
	}

	public long getLogRolloverCheckPeriod() {
		return logRolloverCheckPeriod;
	}

	public void setLogRolloverCheckPeriod(long logRolloverCheckPeriod) {
		this.logRolloverCheckPeriod = logRolloverCheckPeriod;
	}

}