package org.streams.commons.file;

import java.io.Serializable;

/**
 * Represents a synchronisation pointer of an agent file in the
 * CoordinationServer.
 * 
 */
public class SyncPointer implements Comparable<SyncPointer>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String lockId;
	long filePointer;
	int linePointer;
	long fileSize;

	/**
	 * Set by default to System.currentTimeMillis()
	 */
	long timeStamp = System.currentTimeMillis();

	public SyncPointer() {
	}

	public SyncPointer(FileTrackingStatus status) {
		this.filePointer = status.getFilePointer();
		this.linePointer = status.getLinePointer();
		this.fileSize = status.getFileSize();
		this.lockId = new FileTrackingStatusKey(status).getKey();
	}

	public String getLockId() {
		return lockId;
	}

	public void setLockId(String lockId) {
		this.lockId = lockId;
	}

	public long getFilePointer() {
		return filePointer;
	}

	public void setFilePointer(long filePointer) {
		this.filePointer = filePointer;
	}

	public void incLinePointer(int add) {
		linePointer += add;
	}

	public void incFilePointer(long add) {
		filePointer += add;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	@Override
	public int compareTo(SyncPointer o) {
		return lockId.compareTo(o.getLockId());
	}

	public int getLinePointer() {
		return linePointer;
	}

	public void setLinePointer(int linePointer) {
		this.linePointer = linePointer;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((lockId == null) ? 0 : lockId.hashCode());
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
		SyncPointer other = (SyncPointer) obj;
		if (lockId == null) {
			if (other.lockId != null)
				return false;
		} else if (!lockId.equals(other.lockId))
			return false;
		return true;
	}

}
