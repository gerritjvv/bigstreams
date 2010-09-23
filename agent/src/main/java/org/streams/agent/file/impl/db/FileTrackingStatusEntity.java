package org.streams.agent.file.impl.db;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.QueryHint;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.streams.agent.file.FileTrackingStatus;


/**
 * A separate entity class is used to store the actual FileTrackingStatus, this
 * means that the rest of the agent code is not infected with db code in an
 * environment where speed and lightness is of essence.
 * <p/>
 * The DBFileTrackerMemoryImpl is responsible for translating the
 * FiletrackingStatus from and to its Entity.
 */
@Entity
@Table(name = "file_tracking_status", uniqueConstraints = { @UniqueConstraint(columnNames = { "path" }) })
@NamedQueries(value = {
		@NamedQuery(name = "fileTrackingStatus.byStatusReady", query = "from FileTrackingStatusEntity f where f.status='READY'", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileTrackingStatus.byStatus", query = "from FileTrackingStatusEntity f where f.status=:status", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileTrackingStatus.list", query = "from FileTrackingStatusEntity f", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileTrackingStatus.byPathUpdate", query = "from FileTrackingStatusEntity f where f.path=:path"),
		@NamedQuery(name = "fileTrackingStatus.byPath", query = "from FileTrackingStatusEntity f where f.path=:path", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileTrackingStatus.countByStatus", query = "select COUNT(*) as count from FileTrackingStatusEntity f WHERE f.status=:status", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileTrackingStatus.count", query = "select COUNT(*) as count from FileTrackingStatusEntity f", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }) 
	})
public class FileTrackingStatusEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	Long id;
	long lastModificationTime = 0L;
	long fileSize = 0L;

	String path;
	String status;
	int linePointer = 0;
	long filePointer = 0L;
	String logType;

	public FileTrackingStatusEntity(){}
	
	
	public FileTrackingStatusEntity(long lastModificationTime, long fileSize,
			String path, String status, int linePointer, long filePointer,
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


	@Id
	@GeneratedValue(strategy=GenerationType.AUTO)
	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	@Column(name = "last_modification_time", nullable = false)
	public long getLastModificationTime() {
		return lastModificationTime;
	}

	public void setLastModificationTime(long lastModificationTime) {
		this.lastModificationTime = lastModificationTime;
	}

	@Column(name = "file_size", nullable = false)
	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	@Column(name = "path", updatable = false, nullable = false)
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	@Column(name = "status", nullable = false)
	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	@Column(name = "line_pointer", nullable = false)
	public int getLinePointer() {
		return linePointer;
	}

	public void setLinePointer(int linePointer) {
		this.linePointer = linePointer;
	}

	@Column(name = "file_pointer", nullable = false)
	public long getFilePointer() {
		return filePointer;
	}

	public void setFilePointer(long filePointer) {
		this.filePointer = filePointer;
	}

	@Column(name = "log_type", nullable = false)
	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	/**
	 * Creates a FileTrackingStatusEntity instance without an id value
	 * @param fileTrackingStatus
	 * @return
	 */
	public static FileTrackingStatusEntity createEntity(FileTrackingStatus fileTrackingStatus){
		
		return new FileTrackingStatusEntity(fileTrackingStatus.getLastModificationTime(),
				fileTrackingStatus.getFileSize(),
				fileTrackingStatus.getPath(),
				fileTrackingStatus.getStatus().toString().toUpperCase(),
				fileTrackingStatus.getLinePointer(),
				fileTrackingStatus.getFilePointer(),
				fileTrackingStatus.getLogType().toLowerCase());
	}
	
	/**
	 * Updates the internal state of the entity wit hthe FileTrackingStatus values.
	 * @param status
	 */
	public void update(FileTrackingStatus status){
		
		setFilePointer(status.getFilePointer());
		setLastModificationTime(status.getLastModificationTime());
		setFileSize(status.getFileSize());
		setLinePointer(status.getLinePointer());
		setLogType(status.getLogType());
		setPath(status.getPath());
		setStatus(status.getStatus().toString().toUpperCase());
		
	}
	
	/**
	 * Creates a FileTrackingStatus instance
	 * 
	 * @return
	 */
	public FileTrackingStatus createStatusObject() {

		return new FileTrackingStatus(getLastModificationTime(), getFileSize(),
				getPath(), FileTrackingStatus.STATUS.valueOf(getStatus()),
				getLinePointer(), getFilePointer(), getLogType().toLowerCase());

	}

}
