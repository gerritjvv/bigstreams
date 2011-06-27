package org.streams.commons.file;

import java.io.Serializable;

/**
 * Used to represent the composite key that FileTrackingStatus contains made up
 * of : logType + agentName + fileName
 * 
 */
public class FileTrackingStatusKey implements Serializable,
		Comparable<FileTrackingStatusKey> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String agentName = null;
	String logType = null;
	String fileName = null;

	public FileTrackingStatusKey() {
	}

	public FileTrackingStatusKey(String agentName, String logType,
			String fileName) {
		super();
		this.agentName = agentName.trim();
		this.logType = logType.trim();
		this.fileName = fileName.trim();
	}

	public FileTrackingStatusKey(FileStatus.FileTrackingStatus status) {
		this.agentName = status.getAgentName().trim();
		this.logType = status.getLogType().trim();
		this.fileName = status.getFileName().trim();
	}

	public String getAgentName() {
		return agentName;
	}

	public void setAgentName(String agentName) {
		this.agentName = agentName;
	}

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getKey() {
		return logType + agentName + fileName;
	}

	@Override
	public int hashCode() {
		String key = getKey();
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
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
		FileTrackingStatusKey other = (FileTrackingStatusKey) obj;
		String key = getKey();
		String otherKey = other.getKey();

		if (key == null) {
			if (otherKey != null)
				return false;
		} else if (!key.equals(otherKey))
			return false;
		return true;
	}

	@Override
	public int compareTo(FileTrackingStatusKey f) {
		return getKey().compareTo(f.getKey());
	}

}
