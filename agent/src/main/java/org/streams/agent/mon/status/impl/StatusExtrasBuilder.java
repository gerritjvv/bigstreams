package org.streams.agent.mon.status.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileSystemUtils;
import org.apache.log4j.Logger;
import org.streams.agent.conf.AgentProperties;
import org.streams.agent.mon.status.AgentStatus;
import org.streams.commons.group.ExtrasBuilder;
import org.streams.commons.group.Group.GroupStatus.ExtraField;

/**
 * 
 * This class adds in the extras fields for the agent.
 * 
 * 
 */
public class StatusExtrasBuilder implements ExtrasBuilder {

	private static final Logger LOG = Logger
			.getLogger(StatusExtrasBuilder.class);

	AgentStatus status;

	/**
	 * 
	 * @param status
	 *            Agent status. The status is updated by the StatusUpdaterThread
	 */
	public StatusExtrasBuilder(AgentStatus status) {
		super();
		this.status = status;
	}

	@Override
	public List<ExtraField> build() {

		ExtraField lateFiles = ExtraField.newBuilder().setKey("lateFiles")
				.setValue(getLateFiles()).build();
		ExtraField parkedFiles = ExtraField.newBuilder().setKey("parkedFiles")
				.setValue(getParkedFiles()).build();
		ExtraField readyFiles = ExtraField.newBuilder().setKey("readyFiles")
				.setValue(getReadyFiles()).build();
		ExtraField doneFiles = ExtraField.newBuilder().setKey("doneFiles")
				.setValue(getDoneFiles()).build();
		ExtraField diskSpace = ExtraField.newBuilder()
				.setKey("freeDiskSpaceKb").setValue(getDiskSpace()).build();
		ExtraField version = ExtraField.newBuilder().setKey("version")
				.setValue(getVersion()).build();

		return Arrays.asList(lateFiles, parkedFiles, readyFiles, doneFiles,
				diskSpace, version);
	}

	private String getVersion() {
		String version = System.getenv(AgentProperties.AGENT_VERSION);
		if (version == null) {
			version = System.getProperty(AgentProperties.AGENT_VERSION);
		}

		if (version == null) {
			version = "UNKOWN";
		}

		return version;
	}

	private String getDiskSpace() {
		try {
			return String.valueOf(FileSystemUtils.freeSpaceKb("/"));
		} catch (IOException e) {
			LOG.error(e.toString(), e);
			return "-1";
		}
	}

	private String getReadyFiles() {
		return String.valueOf(status.getReadyFiles());
	}

	private String getDoneFiles() {
		return String.valueOf(status.getDoneFiles());
	}

	private String getParkedFiles() {
		return String.valueOf(status.getParkedFiles());
	}

	private String getLateFiles() {
		return String.valueOf(status.getLateFiles());
	}

}
