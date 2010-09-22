package org.streams.collector.write;

import java.io.File;

public interface LogRolloverCheck {

	public boolean shouldRollover(File file);

	public boolean shouldRollover(File file, long fileCreationTime, long lastUpdatedTime);

}
