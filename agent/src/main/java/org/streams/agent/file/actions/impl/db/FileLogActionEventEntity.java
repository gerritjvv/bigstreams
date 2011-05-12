package org.streams.agent.file.actions.impl.db;

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

import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.FileTrackingStatus.STATUS;
import org.streams.agent.file.actions.FileLogActionEvent;

/**
 * 
 * Stores the FileLogActionManager events.
 *
 */
@Entity
@Table(name="file_log_manager_actionevents")
@NamedQueries(value = {
		@NamedQuery(name = "fileLogActionEventEntity.byStatus", query = "from FileLogActionEventEntity f where f.status=:status ORDER BY f.fileDate DESC", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileLogActionEventEntity.list", query = "from FileLogActionEventEntity f ORDER BY f.fileDate DESC", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileLogActionEventEntity.byPath", query = "from FileLogActionEventEntity f where f.path=:path ORDER BY f.lastModificationTime DESC", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileLogActionEventEntity.byDelay", query = "from FileLogActionEventEntity f where f.delay > :delay ORDER BY f.lastModificationTime DESC", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }),
		@NamedQuery(name = "fileLogActionEventEntity.listExpired", query = "from FileLogActionEventEntity f where f.delay > :delay AND ((f.delay*1000) - (:currentTime - f.eventTimeStamp)) < 1 ORDER BY f.lastModificationTime DESC", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") })
})
public class FileLogActionEventEntity implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy=GenerationType.AUTO)
	Long id;
	
	/**
	 * The time in seconds an action should wait before running.<br/>
	 * This value does not affect the object comparison or hash.
	 */
	@Column(name="delay_seconds", nullable=true)
	int delay = 0;
	
	@Column(name="action_name", nullable=false)
	String actionName;
	
	/**
	 * Date that appears in the file name if any.<br/>
	 * This includes up to the file hour
	 */
	@Column(name="file_date", nullable=true)
	Date fileDate;
	/**
	 * Date that the file was sent i.e. its status was marked as DONE.
	 */
	@Column(name="sent_date", nullable=true)
	Date sentDate;

	@Column(name="last_modification_time")
	long lastModificationTime = 0L;
	
	@Column(name="event_time_stamp", nullable=false)
	long eventTimeStamp = 0L;
	
	@Column(name="file_size")
	long fileSize = 0L;
	@Column(name="path", nullable=false)
	String path;
	@Column(name="status", nullable=false)
	STATUS status;
	@Column(name="line_pointer", nullable=false)
	int linePointer = 0;
	@Column(name="file_pointer", nullable=false)
	long filePointer = 0L;
	@Column(name="log_type", nullable=false)
	String logType;
	
	public FileLogActionEventEntity(){
		
	}
	
	/**
	 * Create a FileTrackingStatus instance from the entity
	 * @return FileTrackingStatus
	 */
	public FileTrackingStatus createStatusObject(){
		FileTrackingStatus statusObj = new FileTrackingStatus();
		statusObj.setFileDate(getFileDate());
		statusObj.setFilePointer(getFilePointer());
		statusObj.setFileSize(getFileSize());
		statusObj.setLastModificationTime(getLastModificationTime());
		statusObj.setLinePointer(getLinePointer());
		statusObj.setLogType(getLogType());
		statusObj.setPath(getPath());
		statusObj.setSentDate(getSentDate());
		statusObj.setStatus(getStatus());
		
		return statusObj;
	}
	
	public FileLogActionEvent createEventObject(){
		FileLogActionEvent evt = new FileLogActionEvent(getId(),
				createStatusObject(), actionName, delay);
		evt.setTimeStamp(getEventTimeStamp());
		return evt;
	}
	
	/**
	 * Create a FileLogActionEventEntity entity from then FileTrackingStatus instance
	 * @param status
	 * @return FileLogActionEventEntity
	 */
	public static FileLogActionEventEntity createEntity(FileLogActionEvent event){
		
		FileTrackingStatus status = event.getStatus();
		
		FileLogActionEventEntity entity = new FileLogActionEventEntity();
		entity.setFileDate(status.getFileDate());
		entity.setFilePointer(status.getFilePointer());
		entity.setFileSize(status.getFileSize());
		entity.setLastModificationTime(status.getLastModificationTime());
		entity.setLinePointer(status.getLinePointer());
		entity.setLogType(status.getLogType());
		entity.setPath(status.getPath());
		entity.setSentDate(status.getSentDate());
		entity.setStatus(status.getStatus());
		entity.setActionName(event.getActionName());
		entity.setDelay(event.getDelay());
		entity.setEventTimeStamp(event.getTimeStamp());
		
		return entity;
	}
	
	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Date getFileDate() {
		return fileDate;
	}

	public void setFileDate(Date fileDate) {
		this.fileDate = fileDate;
	}

	public Date getSentDate() {
		return sentDate;
	}

	public void setSentDate(Date sentDate) {
		this.sentDate = sentDate;
	}

	public long getLastModificationTime() {
		return lastModificationTime;
	}

	public void setLastModificationTime(long lastModificationTime) {
		this.lastModificationTime = lastModificationTime;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public STATUS getStatus() {
		return status;
	}

	public void setStatus(STATUS status) {
		this.status = status;
	}

	public int getLinePointer() {
		return linePointer;
	}

	public void setLinePointer(int linePointer) {
		this.linePointer = linePointer;
	}

	public long getFilePointer() {
		return filePointer;
	}

	public void setFilePointer(long filePointer) {
		this.filePointer = filePointer;
	}

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	public int getDelay() {
		return delay;
	}

	public void setDelay(int delay) {
		this.delay = delay;
	}

	public String getActionName() {
		return actionName;
	}

	public void setActionName(String actionName) {
		this.actionName = actionName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((actionName == null) ? 0 : actionName.hashCode());
		result = prime * result
				+ ((fileDate == null) ? 0 : fileDate.hashCode());
		result = prime * result + (int) (filePointer ^ (filePointer >>> 32));
		result = prime * result + (int) (fileSize ^ (fileSize >>> 32));
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result
				+ (int) (lastModificationTime ^ (lastModificationTime >>> 32));
		result = prime * result + linePointer;
		result = prime * result + ((logType == null) ? 0 : logType.hashCode());
		result = prime * result + ((path == null) ? 0 : path.hashCode());
		result = prime * result
				+ ((sentDate == null) ? 0 : sentDate.hashCode());
		result = prime * result + ((status == null) ? 0 : status.hashCode());
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
		FileLogActionEventEntity other = (FileLogActionEventEntity) obj;
		if (actionName == null) {
			if (other.actionName != null)
				return false;
		} else if (!actionName.equals(other.actionName))
			return false;
		if (fileDate == null) {
			if (other.fileDate != null)
				return false;
		} else if (!fileDate.equals(other.fileDate))
			return false;
		if (filePointer != other.filePointer)
			return false;
		if (fileSize != other.fileSize)
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (lastModificationTime != other.lastModificationTime)
			return false;
		if (linePointer != other.linePointer)
			return false;
		if (logType == null) {
			if (other.logType != null)
				return false;
		} else if (!logType.equals(other.logType))
			return false;
		if (path == null) {
			if (other.path != null)
				return false;
		} else if (!path.equals(other.path))
			return false;
		if (sentDate == null) {
			if (other.sentDate != null)
				return false;
		} else if (!sentDate.equals(other.sentDate))
			return false;
		if (status != other.status)
			return false;
		return true;
	}

	public long getEventTimeStamp() {
		return eventTimeStamp;
	}

	public void setEventTimeStamp(long eventTimeStamp) {
		this.eventTimeStamp = eventTimeStamp;
	}
	
}
