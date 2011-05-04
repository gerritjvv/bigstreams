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
import org.streams.coordination.file.AgentContact;

/**
 * 
 * Agent contact db entity
 * 
 */
@Entity
@Table(name = "collector_file_tracking_agent_status")
@NamedQueries(value = { @NamedQuery(name = "agentContact.list", query = "from AgentContactEntity", hints = { @QueryHint(name = "org.hibernate.readOnly", value = "true") }) })
public class AgentContactEntity implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@Column(name = "agent_name", nullable = false)
	String name;

	@Column(name = "last_contact", nullable = false)
	Date timestamp;

	public AgentContactEntity() {
	}

	public AgentContactEntity(FileTrackingStatus status) {
		name = status.getAgentName();
		timestamp = new Date();
	}

	public AgentContactEntity(String name, Date timestamp) {
		super();
		this.name = name;
		this.timestamp = timestamp;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public AgentContact createAgentContact() {
		return new AgentContact(getName(), getTimestamp());
	}

	public static AgentContactEntity createEntity(AgentContact agentContact) {
		return new AgentContactEntity(agentContact.getName(),
				agentContact.getTimestamp());
	}

	public void update(FileTrackingStatus status) {
		setName(status.getAgentName());
		setTimestamp(new Date());
	}

	public void update(AgentContact agentContact) {
		setName(agentContact.getName());
		setTimestamp(agentContact.getTimestamp());
	}

}
