package org.streams.coordination.file.impl.db;

import java.io.Serializable;
import java.util.Date;

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

import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.FileTrackingStatusKey;


/**
 * Persistence for the FileTrackingStatus object.<br/>
 * This object is only to be used by the DBCollectorFileTrackerMemory.
 */
@Entity
@Table(name = "collector_file_tracking_status", uniqueConstraints = { @UniqueConstraint(columnNames = {
		"agent", "file_name", "log_type" }) })
@NamedQueries(value = {
		@NamedQuery(name = "fileTrackingStatus.byFileReadOnly", query = "from FileTrackingStatusEntity f where f.fileName=:fileName", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileTrackingStatus.byAgentFileReadOnly", query = "from FileTrackingStatusEntity f where f.agentName=:agentName and f.fileName=:fileName", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileTrackingStatus.byLogTypeReadOnly", query = "from FileTrackingStatusEntity f where f.logType=:logType", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileTrackingStatus.byAgentLogTypeReadOnly", query = "from FileTrackingStatusEntity f where f.agentName=:agentName and f.logType=:logType", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),

		@NamedQuery(name = "fileTrackingStatus.byAgentFileNameLogTypeReadOnly", query = "from FileTrackingStatusEntity f where f.agentName=:agentName and f.fileName=:fileName and f.logType=:logType", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),

		@NamedQuery(name = "fileTrackingStatus.byAgentLogTypeFileUpdate", query = "from FileTrackingStatusEntity f where f.agentName=:agentName and f.fileName=:fileName and f.logType=:logType"),
		@NamedQuery(name = "fileTrackingStatus.byAgentNameReadOnly", query = "from FileTrackingStatusEntity f where f.agentName=:agentName", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),

		@NamedQuery(name = "fileTrackingStatus.listAgents", query = "select DISTINCT(agentName) from FileTrackingStatusEntity f", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileTrackingStatus.listLogTypes", query = "select DISTINCT(logType) from FileTrackingStatusEntity f", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileTrackingStatus.list", query = "from FileTrackingStatusEntity", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),

		@NamedQuery(name = "fileTrackingStatus.countLogTypes", query = "select COUNT(DISTINCT logType) from FileTrackingStatusEntity f", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileTrackingStatus.countAgents", query = "select COUNT(DISTINCT agentName) from FileTrackingStatusEntity f", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileTrackingStatus.countByAgentName", query = "select COUNT(fileName) from FileTrackingStatusEntity f WHERE f.agentName=:agentName", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileTrackingStatus.count", query = "select COUNT(*) from FileTrackingStatusEntity f", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }) })
public class FileTrackingStatusEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	Date date;
	Long id;
	long fileSize = 0L;
	long filePointer = 0L;
	int linePointer = 0;

	long lastModifiedTime = 0L;

	String agentName;
	String fileName;
	String logType;

	Date fileDate;
	
	public FileTrackingStatusEntity() {
		date = new Date();
	}

	public FileTrackingStatusEntity(Date date, long filePointer, long fileSize,
			int linePointer, String agentName, String fileName, String logType, Date fileDate) {
		super();
		this.date = date;
		this.fileSize = fileSize;
		this.filePointer = filePointer;
		this.linePointer = linePointer;
		this.agentName = agentName;
		this.fileName = fileName;
		this.logType = logType;
		this.fileDate = fileDate;
	}

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	@Column(name = "file_size", nullable = false)
	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	@Column(name = "file_pointer", nullable = false)
	public long getFilePointer() {
		return filePointer;
	}

	public void setFilePointer(long filePointer) {
		this.filePointer = filePointer;
	}

	@Column(name = "agent", nullable = false)
	public String getAgentName() {
		return agentName;
	}

	public void setAgentName(String agentName) {
		this.agentName = agentName;
	}

	@Column(name = "file_name", nullable = false)
	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	@Column(name = "log_type", nullable = false)
	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	@Column(name = "last_modified", nullable = false)
	public long getLastModifiedTime() {
		return lastModifiedTime;
	}

	public void setLastModifiedTime(long lastModifiedTime) {
		this.lastModifiedTime = lastModifiedTime;
	}

	@Column(name = "line_pointer", nullable = false)
	public int getLinePointer() {
		return linePointer;
	}

	public void setLinePointer(int linePointer) {
		this.linePointer = linePointer;
	}

	/**
	 * Creates a FileTrackingStatus instance from the attributes in this
	 * instance.
	 * 
	 * @return
	 */
	public FileTrackingStatus createStatusObject() {
		return new FileTrackingStatus(date, filePointer, fileSize, linePointer, agentName,
				fileName, logType, fileDate, lastModifiedTime);
	}

	/**
	 * Creates a FileTrackingStatusKey instance from the attributes in this
	 * instance.
	 * 
	 * @return
	 */
	public FileTrackingStatusKey createStatusKeyObject() {
		return new FileTrackingStatusKey(agentName, logType, fileName);
	}

	
	/**
	 * Creates a FileTrackingStatusEntity instance from the values of the
	 * FileTrackingStatus passed as parameter.
	 * 
	 * @param status
	 * @return
	 */
	public static FileTrackingStatusEntity createEntity(
			FileTrackingStatus status) {
		return new FileTrackingStatusEntity(status.getDate(), status.getFilePointer(),
				status.getFileSize(), status.getLinePointer(),
				status.getAgentName(), status.getFileName(),
				status.getLogType(), status.getFileDate());
	}

	
	/**
	 * Copies the values from the FileTrackingStatus into the attributes of this
	 * instance.<br/>
	 * 
	 * @param status
	 */
	public void update(FileTrackingStatus status) {
		setFilePointer(status.getFilePointer());
		setFileSize(status.getFileSize());
		setAgentName(status.getAgentName());
		setFileName(status.getFileName());
		setLogType(status.getLogType());
		setLinePointer(status.getLinePointer());
		setDate(status.getDate());
		setFileDate(status.getFileDate());
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

	@Column(name = "file_date", nullable = true)
	public void setFileDate(Date fileDate) {
		this.fileDate = fileDate;
	}

}
