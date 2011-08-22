package org.streams.collector.conf;

import org.apache.commons.configuration.Configuration;

/**
 * 
 * Configuration of what to do when disk space runs out.
 *
 */
public class DiskSpaceFullActionConf {

	public static enum ACTION { DIE, ALERT }
    
	ACTION action;
	long diskFullKBActivation;
	
	public DiskSpaceFullActionConf(Configuration conf){
		
		action = ACTION.valueOf(conf.getString(CollectorProperties.WRITER.DISK_FULL_ACTION.toString(),
				(String)CollectorProperties.WRITER.DISK_FULL_ACTION.getDefaultValue()));
		
		diskFullKBActivation = conf.getLong(CollectorProperties.WRITER.DISK_FULL_KB_ACTIVATION.toString(),
				(Long)CollectorProperties.WRITER.DISK_FULL_KB_ACTIVATION.getDefaultValue());
		
	}
	
	public ACTION getDiskAction(){return action;}

	
	public long getDiskFullKBActivation() {
		return diskFullKBActivation;
	}
	
	
}
