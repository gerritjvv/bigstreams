package org.streams.commons.io;

import java.util.Date;

import org.codehaus.jackson.map.ObjectMapper;

public class Header {

	public static final int HEADER_MAGIC_NUMB = 0x10;

	private static final ObjectMapper mapper = new ObjectMapper();

	String host;
	String fileName;
	String logType;
	/**
	 * The compression codec class name
	 */
	String codecClassName;

	long fileSize = 0L;
	long filePointer = 0L;
	int linePointer = 0;
	long uniqueId;
	
	Date fileDate;
	
	public Header() {
	}

	public Header(String host, String fileName, String logType, long uniqueId,
			String codecClassName, long filePointer, long fileSize, int linePointer, Date fileDate) {
		super();
		this.host = host;
		this.fileName = fileName;
		this.logType = logType;
		this.uniqueId = uniqueId;
		this.codecClassName = codecClassName;
		this.filePointer = filePointer;
		this.fileSize = fileSize;
		this.linePointer = linePointer;
		this.fileDate = fileDate;
	}

	
	public String getCodecClassName() {
		return codecClassName;
	}

	public void setCodecClassName(String codecClassName) {
		this.codecClassName = codecClassName;
	}

	public Date getFileDate() {
		return fileDate;
	}

	public void setFileDate(Date fileDate) {
		this.fileDate = fileDate;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	public long getUniqueId() {
		return uniqueId;
	}

	public void setUniqueId(long uniqueId) {
		this.uniqueId = uniqueId;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public String toJsonString() {
		try {
			return mapper.writeValueAsString(this);
		} catch (Throwable t) {
			RuntimeException e = new RuntimeException(t.toString(), t);
			e.setStackTrace(t.getStackTrace());
			throw e;
		}
	}

	public long getFilePointer() {
		return filePointer;
	}

	public void setFilePointer(long filePointer) {
		this.filePointer = filePointer;
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
				+ ((fileName == null) ? 0 : fileName.hashCode());
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + ((logType == null) ? 0 : logType.hashCode());
		result = prime * result + (int) (uniqueId ^ (uniqueId >>> 32));
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
		Header other = (Header) obj;
		if (fileName == null) {
			if (other.fileName != null)
				return false;
		} else if (!fileName.equals(other.fileName))
			return false;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (logType == null) {
			if (other.logType != null)
				return false;
		} else if (!logType.equals(other.logType))
			return false;
		if (uniqueId != other.uniqueId)
			return false;
		return true;
	}

}
