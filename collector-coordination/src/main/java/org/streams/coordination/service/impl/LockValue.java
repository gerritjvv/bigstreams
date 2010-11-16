package org.streams.coordination.service.impl;

import java.io.Serializable;

import org.streams.commons.file.FileTrackingStatus;

public class LockValue implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	FileTrackingStatus status;
	String remoteAddress;
	long timeStamp = System.currentTimeMillis();

	public FileTrackingStatus getStatus() {
		return status;
	}

	public void setStatus(FileTrackingStatus status) {
		this.status = status;
	}

	public String getRemoteAddress() {
		return remoteAddress;
	}

	public void setRemoteAddress(String remoteAddress) {
		this.remoteAddress = remoteAddress;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

}
