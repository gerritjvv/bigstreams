package org.streams.collector.write;

import java.io.File;
import java.io.IOException;

public interface LogRollover {

	public File rollover(File file) throws IOException;
	
}
