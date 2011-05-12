package org.streams.coordination.mon.impl;

import java.util.Comparator;
import java.util.Date;

import org.streams.commons.file.FileTrackingStatus;

/**
 * Compares FileTrackingStatus instances by file date
 * 
 *
 */
public class FileDateComparator implements Comparator<FileTrackingStatus>{

	public static final FileDateComparator INSTANCE = new FileDateComparator();
	
	@Override
	public int compare(FileTrackingStatus o1, FileTrackingStatus o2) {
		Date d1 = o1.getFileDate();
		Date d2 = o2.getFileDate();
		
		if(d1 == null || d2 == null){
			return 0;
		}else{
			return d1.compareTo(d2);
		}
		
	}

	
	
}
