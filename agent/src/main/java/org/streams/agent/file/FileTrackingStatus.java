package org.streams.agent.file;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * Stores the status of a file being read.<br/>
 * Status is defined as:<br/>
 * <ul>
 * <li>READY : the file has been seen and is ready to be read</li>
 * <li>READING: some process is busy reading the file</li>
 * <li>DELETED: the files has been deleted</li>
 * <li>CHANGED: this is an error in that the file has changed since it was seen.
 * </li>
 * <li>DONE: the agent has finished sending the file.</li>
 * <li>READ_ERROR: any error that ocurred while reading the file.</li>
 * </ul>
 * <p/>
 * File pointers:<br/>
 * The agent will be reading batches of lines or bytes from a file and update
 * the linePointer and filePointer accordingly.
 */
public class FileTrackingStatus implements Cloneable{

	public static enum STATUS {
		READY, READING, DELETED, CHANGED, DONE, READ_ERROR
	}

	long lastModificationTime = 0L;
	long fileSize = 0L;
	String path;
	STATUS status;
	int linePointer = 0;
	long filePointer = 0L;
	String logType;

	public FileTrackingStatus() {
	}

	public FileTrackingStatus(long lastModificationTime, long fileSize,
			String path, STATUS status, int linePointer, long filePointer,
			String logType) {
		super();
		this.lastModificationTime = lastModificationTime;
		this.fileSize = fileSize;
		this.path = path;
		this.status = status;
		this.linePointer = linePointer;
		this.filePointer = filePointer;
		this.logType = logType;
	}

	/**
	 * Log type being sent
	 * 
	 * @return
	 */
	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	/**
	 * Byte count of the next byte that should be read from the file.<br/>
	 * i.e. all bytes up to getFilePointer()-1 have been read inclusively.
	 * 
	 * @return
	 */
	public long getFilePointer() {
		return filePointer;
	}

	public void setFilePointer(long filePointer) {
		this.filePointer = filePointer;
	}

	public long getLastModificationTime() {
		return lastModificationTime;
	}

	/**
	 * Modification time of the file
	 * 
	 * @param lastModificationTime
	 */
	public void setLastModificationTime(long lastModificationTime) {
		this.lastModificationTime = lastModificationTime;
	}

	/**
	 * Site in bytes of the file
	 * 
	 * @return
	 */
	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	/**
	 * The absolute path of the file.
	 * 
	 * @return
	 */
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	/**
	 * The status in which the file is i.e. READING, DONE etc.
	 * 
	 * @return
	 */
	public STATUS getStatus() {
		return status;
	}

	public void setStatus(STATUS status) {
		this.status = status;
	}

	public int getLinePointer() {
		return linePointer;
	}

	public void setLinePointer(int bytePointer) {
		this.linePointer = bytePointer;
	}

	public Object clone(){
		FileTrackingStatus file = new FileTrackingStatus();
		file.setPath(path);
		file.setFilePointer(filePointer);
		file.setFileSize(fileSize);
		file.setLastModificationTime(lastModificationTime);
		file.setLinePointer(linePointer);
		file.setLogType(logType);
		file.setStatus(status);
		
		return file;
	}
	
	/**
	 * Reads data from a key[=:]value comma/semi comma seperated string
	 * 
	 * @param str
	 */
	public void fill(String str) {

		// convert to a map
		String[] keyValueSplits = str.split("[,;]");
		Map<String, String> values = new HashMap<String, String>();

		if (keyValueSplits != null) {

			for (String keyValue : keyValueSplits) {

				String[] split = keyValue.split("[=:]");

				if (split != null && split.length == 2) {
					values.put(split[0], split[1]);
				}
			}
			
			//only change those values that are specified in the values map
			if (values.size() > 0) {

				if (values.containsKey("lastModificationTime")) {
					lastModificationTime = Long.valueOf(values
							.get("lastModificationTime"));
				}
				if (values.containsKey("fileSize")) {
					fileSize = Long.valueOf(values.get("fileSize"));
				}
				if (values.containsKey("filePointer")) {
					filePointer = Long.valueOf(values.get("filePointer"));
				}
				if (values.containsKey("path")) {
					path = values.get("path");
				}
				if (values.containsKey("logType")) {
					logType = values.get("logType");
				}
				if (values.containsKey("linePointer")) {
					linePointer = Integer.parseInt(values.get("linePointer"));
				}
				if (values.containsKey("status")) {
					status = STATUS.valueOf(values.get("status"));
				}
			}

		}

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((path == null) ? 0 : path.hashCode());
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
		if (path == null) {
			if (other.path != null)
				return false;
		} else if (!path.equals(other.path))
			return false;
		return true;
	}

}
