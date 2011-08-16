package org.streams.collector.mon.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileSystemUtils;
import org.apache.log4j.Logger;
import org.streams.collector.mon.CollectorStatus;
import org.streams.commons.group.ExtrasBuilder;
import org.streams.commons.group.Group.GroupStatus.ExtraField;
import org.streams.commons.metrics.CounterMetric;

public class StatusExtrasBuilder implements ExtrasBuilder {

	private static final Logger LOG = Logger
			.getLogger(StatusExtrasBuilder.class);

	CollectorStatus status;
	String dataDir;

	CounterMetric connectionsReceived = null;
	CounterMetric connectionsProcessed = null;
	CounterMetric kilobytesWrittenMetric = null;
	CounterMetric errorsMetric = null;

	
	public StatusExtrasBuilder(CollectorStatus status, String dataDir,
			CounterMetric connectionsReceived,
			CounterMetric connectionsProcessed,
			CounterMetric kilobytesWrittenMetric, CounterMetric errorsMetric) {
		super();
		this.status = status;
		this.dataDir = dataDir;
		this.connectionsReceived = connectionsReceived;
		this.connectionsProcessed = connectionsProcessed;
		this.kilobytesWrittenMetric = kilobytesWrittenMetric;
		this.errorsMetric = errorsMetric;
	}

	@Override
	public List<ExtraField> build() {

		ExtraField version = ExtraField.newBuilder().setKey("version")
				.setValue(status.getVersion()).build();
		ExtraField extConnectionsReceived = ExtraField.newBuilder().setKey("connectionsReceived")
		.setValue(String.valueOf(connectionsReceived.getValue())).build();
		
		ExtraField extConnectionsProcessed = ExtraField.newBuilder().setKey("connectionsProcessed")
		.setValue(String.valueOf(connectionsProcessed.getValue())).build();
		
		ExtraField extKilobytesWrittenMetric = ExtraField.newBuilder().setKey("kilobytesWritten")
		.setValue(String.valueOf(kilobytesWrittenMetric.getValue())).build();
		
		ExtraField extErrorsMetric = ExtraField.newBuilder().setKey("errors")
		.setValue(String.valueOf(errorsMetric.getValue())).build();
		
		
		ExtraField freeSpace = ExtraField.newBuilder()
				.setKey("freeDiskSpaceKb").setValue(getDiskSpace()).build();
		ExtraField openFiles = ExtraField
				.newBuilder()
				.setKey("openFiles")
				.setValue(
						String.valueOf(status
								.getCounter(CollectorStatus.COUNTERS.FILES_OPEN
										.toString()))).build();

		return Arrays.asList(extConnectionsProcessed, extConnectionsReceived, extKilobytesWrittenMetric, extErrorsMetric, openFiles, freeSpace, version);

	}

	private String getDiskSpace() {
		try {
			return String.valueOf(FileSystemUtils.freeSpaceKb(dataDir));
		} catch (IOException e) {
			LOG.error(e.toString(), e);
			return "-1";
		}
	}

}
