package org.streams.commons.file;

import java.io.Serializable;
import java.util.Date;

/**
 * Represents a snapshot of the file data
 * 
 */
public class FileTrackingStatus implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * This is the last modified or read date.
	 * E.g. if used by the coordination service, this means the last read date.
	 */
	Date date;
	long filePointer;
	long fileSize;
	
	long lastModifiedTime;
	
	int linePointer;
	
	String agentName;
	String fileName;
	String logType;

	/**
	 * The file date has no influence on the hashcode equals or other.
	 */
	Date fileDate;
	
	public FileTrackingStatus() {
		date = new Date();
	}

	public FileTrackingStatus(Date date, long filePointer, long fileSize,int linePointer,
			String agentName, String fileName, String logType, Date fileDate) {
		super();
		this.date = date;
		this.filePointer = filePointer;
		this.fileSize = fileSize;
		this.agentName = agentName;
		this.fileName = fileName;
		this.logType = logType;
		this.linePointer = linePointer;
		this.fileDate = fileDate;
	}

	public long getFilePointer() {
		return filePointer;
	}

	public void setFilePointer(long filePointer) {
		this.filePointer = filePointer;
	}

	public String getAgentName() {
		return agentName;
	}

	public void setAgentName(String agentName) {
		this.agentName = agentName;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	public int getLinePointer() {
		return linePointer;
	}

	public void setLinePointer(int linePointer) {
		this.linePointer = linePointer;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((agentName == null) ? 0 : agentName.hashCode());
		result = prime * result
				+ ((fileName == null) ? 0 : fileName.hashCode());
		result = prime * result + ((logType == null) ? 0 : logType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FileTrackingStatus other = (FileTrackingStatus) obj;
		if (agentName == null) {
			if (other.agentName != null)
				return false;
		} else if (!agentName.equals(other.agentName))
			return false;
		if (fileName == null) {
			if (other.fileName != null)
				return false;
		} else if (!fileName.equals(other.fileName))
			return false;
		if (logType == null) {
			if (other.logType != null)
				return false;
		} else if (!logType.equals(other.logType))
			return false;
		return true;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public Date getFileDate() {
		return fileDate;
	}

	public void setFileDate(Date fileDate) {
		this.fileDate = fileDate;
	}

	public long getLastModifiedTime() {
		return lastModifiedTime;
	}

	public void setLastModifiedTime(long lastModifiedTime) {
		this.lastModifiedTime = lastModifiedTime;
	}

}
