package org.streams.collector.write;

import java.io.File;
import java.util.List;

public interface OrphanedFilesCheck {

	/**
	 * Search for orphaned files and roll them.
	 * @return List of Files rolled that is the rolled files.
	 * @throws InterruptedException 
	 */
	List<File> rollFiles() throws InterruptedException;
	
}
