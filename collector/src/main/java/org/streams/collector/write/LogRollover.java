package org.streams.collector.write;

import java.io.File;
import java.io.IOException;

public interface LogRollover {

	public File rollover(File file) throws IOException;
	
	/**
	 * Returns true if the file appears to have been rolled
	 * @param file
	 * @return
	 */
	public boolean isRolledFile(File file);
	
}
