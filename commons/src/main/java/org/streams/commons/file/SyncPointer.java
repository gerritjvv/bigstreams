package org.streams.commons.file;

/**
 * Represents a synchronisation pointer of an agent file in the
 * CoordinationServer.
 * 
 */
public class SyncPointer implements Comparable<SyncPointer>{

	int lockId;
	long filePointer;
	int linePointer;
	long fileSize;
	
	public int getLockId() {
		return lockId;
	}
	
	public void setLockId(int lockId) {
		this.lockId = lockId;
	}
	
	public long getFilePointer() {
		return filePointer;
	}
	public void setFilePointer(long filePointer) {
		this.filePointer = filePointer;
	}

	public void incLinePointer(int add){
		linePointer += add;
	}
	
	public void incFilePointer(long add){
		filePointer += add;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (lockId ^ (lockId >>> 32));
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
		if (lockId != other.lockId)
			return false;
		return true;
	}
	public long getFileSize() {
		return fileSize;
	}
	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}
	
	@Override
	public int compareTo(SyncPointer o) {
		if(o.lockId == lockId){
			return 0;
		}else if(o.lockId < lockId){
			return -1;
		}else{
			return 1;
		}
	}
	public int getLinePointer() {
		return linePointer;
	}
	public void setLinePointer(int linePointer) {
		this.linePointer = linePointer;
	}
	
}
