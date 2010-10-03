package org.streams.collector.write;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * Output stream with rollback and mark support.
 * 
 */
public class RollBackOutputStream extends OutputStream {

	private static final Logger LOG = Logger
			.getLogger(RollBackOutputStream.class);

	OutputStream out;
	File file;

	StreamCreator streamCreator;

	/**
	 * Maintains the byte position of the file Its initial value is
	 * file.length();
	 */
	final AtomicLong position;

	/**
	 * Default is -1, set by the mark() method
	 */
	long mark = -1;

	/**
	 * Sets the position to file.length().
	 * 
	 * @param file
	 * @param out
	 * @param streamCreator
	 * @param initialSize
	 */
	public RollBackOutputStream(File file, OutputStream out,
			StreamCreator streamCreator, long initialSize) {
		super();
		this.file = file;
		this.out = out;
		this.streamCreator = streamCreator;
		position = new AtomicLong(initialSize);
	}

	@Override
	public void write(int b) throws IOException {
		out.write(b);
		position.addAndGet(1);
	}

	/**
	 * assigns the current position value to the mark variable.<br/>
	 * And calls a markEvent to the StreamCreator instance.
	 */
	public void mark() {
		mark = position.get();
		streamCreator.markEvent(file, out);
	}

	/**
	 * Roll-back the file to the last mark position.
	 * 
	 * @throws IOException
	 */
	public void rollback() throws IOException {

		if (mark < 0) {
			throw new IOException("The mark method must be called first");
		}

		LOG.info("Rollback " + file.getAbsolutePath() + " from "
				+ position.get() + " to " + mark);

		// create new roll back file
		File rollbackFile = new File(file + "_" + System.nanoTime()
				+ "-rollback");
		// if error here the file is not created try twice
		if(!rollbackFile.createNewFile() && !rollbackFile.createNewFile()){
			throw new IOException("Failed to create file " + rollbackFile);
		}
		
		
		File intermediate = new File(file + "_" + System.nanoTime()
				+ "-intermediate");

		// close current file, and release any external resource e.g.
		// compression reasource obtained.
		streamCreator.close(file, out);
		out = null;

		// copy current file to roll back file up to last mark
		try {
			streamCreator.transfer(file, rollbackFile, mark);

		} catch (Throwable t) {
			FileUtils.forceDelete(rollbackFile);
			throw new IOException(t.toString(), t);
		}

		// At this stage we have 2 files one is the truncated version and the
		// other the original file.

		// swap names of roll back file to that of current file
		file.renameTo(intermediate);
		if( ! rollbackFile.renameTo(file) ){
			throw new IOException("Error renaming file " + file.getAbsolutePath() + " to " + intermediate.getAbsolutePath());
		}

		// delete old file
		FileUtils.deleteQuietly(intermediate);

		position.set(mark);
		mark = 0;

		out = streamCreator.create(file);
	}

	@Override
	public void write(byte[] b) throws IOException {
		out.write(b);
		position.addAndGet(b.length);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		out.write(b, off, len);
		position.addAndGet(len);
	}

	@Override
	public void flush() throws IOException {
		out.flush();
	}

	@Override
	public void close() throws IOException {
		out.close();
	}

	public OutputStream getOut() {
		return out;
	}

	public File getFile() {
		return file;
	}

	public AtomicLong getPosition() {
		return position;
	}

	public long getMark() {
		return mark;
	}

}
