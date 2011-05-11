package org.streams.agent.file.actions;

import java.util.Comparator;

import org.streams.agent.file.FileTrackingStatus;

/**
 * Compares the FileLogActionEvent by lastModificationTime parameter of its containing FileTrackingStatus.<br/>
 * If the FileTrackingStatus is null 0 is taken as the last modification time.
 *
 */
public class LastModificationTimeComparator  implements Comparator<FileLogActionEvent>{

	public static final LastModificationTimeComparator INSTANCE = new LastModificationTimeComparator();
	
	@Override
	public int compare(FileLogActionEvent o1, FileLogActionEvent o2) {
		
		FileTrackingStatus stat1 = o1.getStatus();
		FileTrackingStatus stat2 = o2.getStatus();
		
		long mod1Time = (stat1 == null) ? 0 : stat1.getLastModificationTime();
		long mod2Time = (stat2 == null) ? 0 : stat2.getLastModificationTime();
		
		if(mod1Time < mod2Time) return -1;
		else if(mod1Time > mod2Time) return 1;
		else return 0;
		
	}

}
