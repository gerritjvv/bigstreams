package org.streams.commons.group;

import java.net.InetAddress;
import java.net.UnknownHostException;
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

	private static final Logger LOG = Logger.getLogger(GroupHeartbeatService.class);
	
	ScheduledExecutorService service = Executors
			.newSingleThreadScheduledExecutor();

	long initialDelay;
	// 24 hours in milliseconds
	long frequency;

	long timeout;

	GroupKeeper groupKeeper;
	Group.GroupStatus.Type type;

	String hostName;
	
	Status status;
	
	public GroupHeartbeatService(GroupKeeper groupKeeper, Group.GroupStatus.Type type,
			Status status,
			long initialDelay,
			long frequency, long timeout) throws UnknownHostException {

		this.groupKeeper = groupKeeper;
		this.type = type;
		this.status = status;
		this.initialDelay = initialDelay;
		this.frequency = frequency;
		this.timeout = timeout;

		InetAddress localMachine = null;
		try {
			localMachine = InetAddress.getLocalHost();
		} catch (UnknownHostException hostexp) {
			LOG.error(hostexp.toString(), hostexp);
			localMachine = InetAddress.getByName("localhost");
		}

		hostName = localMachine.getHostName();

	}

	private static final Group.GroupStatus.Status getStatus(Status.STATUS progStat){
		try{
		return Group.GroupStatus.Status.valueOf(progStat.toString());
		}catch(Throwable t){
			LOG.error(t.toString(), t);
			return Group.GroupStatus.Status.UNKOWN_ERROR;
		}
	}
	
	@Override
	public void start() throws Exception {
		service.scheduleWithFixedDelay(new Runnable() {

			public void run() {
				try{
					String msg = status.getStatusMessage();
					
					
					GroupStatus groupStatus = GroupStatus.newBuilder()
									.setStatus(getStatus(status.getStatus()))
									.setMsg(msg)
									.setLastUpdate(System.currentTimeMillis())
									.setType(type)
									.setHost(hostName)
									.build();
					
					groupKeeper.updateStatus(groupStatus);
					LOG.info("Heartbeat -- " + status.getStatus());
				}catch(Throwable t){
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
