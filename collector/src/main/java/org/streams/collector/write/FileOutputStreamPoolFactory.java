package org.streams.collector.write;

import java.io.IOException;

/**
 * 
 *  Creates or re-uses instances of the FileOutputStreamPool.<br/>
 *
 */
public interface FileOutputStreamPoolFactory {

	FileOutputStreamPool getPoolForKey(String key);
	
	void closeAll();
	
	void checkFilesForRollover(LogRolloverCheck rolloverCheck)
	throws IOException;
}
