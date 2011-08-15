package org.streams.commons.group;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.streams.commons.app.ApplicationService;
import org.streams.commons.group.Group.GroupStatus;
import org.streams.commons.status.Status;
import org.streams.commons.status.Status.STATUS;

/**
 * Sends status updates to zookeeper
 */
public class GroupHeartbeatService implements ApplicationService {

	private static final Logger LOG = Logger
			.getLogger(GroupHeartbeatService.class);

	ScheduledExecutorService service = Executors
			.newSingleThreadScheduledExecutor();

	long initialDelay;
	// 24 hours in milliseconds
	long frequency;

	long timeout;

	GroupKeeper groupKeeper;
	Group.GroupStatus.Type type;

	String hostName;
	int port;
	Status status;

	long lastStatusChange = 0L;

	ExtrasBuilder extrasBuilder;

	public GroupHeartbeatService(GroupKeeper groupKeeper,
			Group.GroupStatus.Type type, Status status, int port,
			long initialDelay, long frequency, long timeout)
			throws UnknownHostException {
		this(groupKeeper, type, status, port, initialDelay, frequency, timeout,
				null);
	}

	public GroupHeartbeatService(GroupKeeper groupKeeper,
			Group.GroupStatus.Type type, Status status, int port,
			long initialDelay, long frequency, long timeout,
			ExtrasBuilder extrasBuilder) throws UnknownHostException {

		this.groupKeeper = groupKeeper;
		this.type = type;
		this.status = status;
		this.port = port;
		this.initialDelay = initialDelay;
		this.frequency = frequency;
		this.timeout = timeout;
		this.extrasBuilder = extrasBuilder;

		InetAddress localMachine = null;
		try {
			localMachine = InetAddress.getLocalHost();
		} catch (UnknownHostException hostexp) {
			LOG.error(hostexp.toString(), hostexp);
			localMachine = InetAddress.getByName("localhost");
		}

		hostName = localMachine.getHostName();

	}

	private final Group.GroupStatus.Status getStatus(Status.STATUS progStat) {
		try {

			// we want the heartbeat status to reset itself if the last status
			// did not change.
			if (status.getStatusTimestamp() == lastStatusChange) {
				progStat = Status.STATUS.OK;
			}

			lastStatusChange = status.getStatusTimestamp();

			return Group.GroupStatus.Status.valueOf(progStat.toString());
		} catch (Throwable t) {
			LOG.error(t.toString(), t);
			return Group.GroupStatus.Status.UNKOWN_ERROR;
		}
	}

	@Override
	public void start() throws Exception {
		service.scheduleWithFixedDelay(new Runnable() {

			public void run() {
				try {
					String msg = status.getStatusMessage();

					// if the hearbeat error was set previously we need to
					// change it to OK.
					Group.GroupStatus.Status gstat = getStatus(status
							.getStatus());

					if (gstat.equals(Group.GroupStatus.Status.HEARTBEAT_ERROR)) {
						gstat = Group.GroupStatus.Status.OK;
						status.setStatus(STATUS.OK, "Working");
					}

					List<GroupStatus.ExtraField> extrasList = null;
					if (extrasBuilder != null) {
						try {
							extrasList = extrasBuilder.build();
						} catch (Throwable t) {
							LOG.error(t.toString(), t);
						}
					}

					GroupStatus.Builder groupStatusBuilder = GroupStatus
							.newBuilder().setStatus(gstat).setMsg(msg)
							.setLastUpdate(System.currentTimeMillis())
							.setType(type).setHost(hostName).setPort(port);
					if (extrasList != null) {
						groupStatusBuilder.addAllExtraField(extrasList);
					}

					GroupStatus groupStatus = groupStatusBuilder.build();

					groupKeeper.updateStatus(groupStatus);
					LOG.info("Heartbeat -- " + status.getStatus());
				} catch (Throwable t) {
					status.setStatus(STATUS.HEARTBEAT_ERROR, t.toString());
					LOG.error(t.toString(), t);
				}
			}

		}, initialDelay, frequency, TimeUnit.MILLISECONDS);

	}

	@Override
	public void shutdown() {
		service.shutdown();
		try {
			service.awaitTermination(500, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
		service.shutdownNow();
	}

}
