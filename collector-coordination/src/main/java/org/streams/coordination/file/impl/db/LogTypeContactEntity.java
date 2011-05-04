package org.streams.coordination.file.impl.db;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.QueryHint;
import javax.persistence.Table;

import org.streams.commons.file.FileTrackingStatus;
import org.streams.coordination.file.LogTypeContact;

/**
 * 
 * Agent contact db entity
 * 
 */
@Entity
@Table(name = "collector_file_tracking_logtype_status")
@NamedQueries(value = { @NamedQuery(name = "logTypeContact.list", query = "from LogTypeContactEntity", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") })})
public class LogTypeContactEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name = "log_type", nullable = false)
	String name;

	@Column(name = "agent_name", nullable = false)
	String agent;

	@Column(name = "last_contact", nullable = false)
	Date timestamp;

	public LogTypeContactEntity() {

	}
	
	public LogTypeContactEntity(FileTrackingStatus status){
		name = status.getLogType();
		agent = status.getAgentName();
		timestamp = new Date();
	}
	
	public LogTypeContactEntity(String name, String agent, Date timestamp) {
		super();
		this.name = name;
		this.agent = agent;
		this.timestamp = timestamp;
	}

	public LogTypeContact createLogTypeContact() {
		return new LogTypeContact(getName(), getAgent(), getTimestamp());
	}

	public static LogTypeContactEntity createEntity(
			LogTypeContact logTypeContact) {
		return new LogTypeContactEntity(logTypeContact.getName(),
				logTypeContact.getAgentName(), logTypeContact.getTimestamp());
	}

	public void update(FileTrackingStatus status) {
		setName(status.getLogType());
		setAgent(status.getAgentName());
		setTimestamp(new Date());
	}
	
	public void update(LogTypeContact logTypeContact) {
		setName(logTypeContact.getName());
		setAgent(logTypeContact.getAgentName());
		setTimestamp(logTypeContact.getTimestamp());
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAgent() {
		return agent;
	}

	public void setAgent(String agent) {
		this.agent = agent;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

}
