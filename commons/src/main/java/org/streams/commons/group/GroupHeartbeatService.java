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
	int port;
	Status status;
	
	public GroupHeartbeatService(GroupKeeper groupKeeper, Group.GroupStatus.Type type,
			Status status, int port,
			long initialDelay,
			long frequency, long timeout) throws UnknownHostException {

		this.groupKeeper = groupKeeper;
		this.type = type;
		this.status = status;
		this.port = port;
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
					
					//if the hearbeat error was set previously we need to change it to OK.
					Group.GroupStatus.Status gstat = getStatus(status.getStatus());
					
					if(gstat.equals(Group.GroupStatus.Status.HEARTBEAT_ERROR)){
						gstat = Group.GroupStatus.Status.OK;
						status.setStatus(STATUS.OK, "Working");
					}
					
					GroupStatus groupStatus = GroupStatus.newBuilder()
									.setStatus(gstat)
									.setMsg(msg)
									.setLastUpdate(System.currentTimeMillis())
									.setType(type)
									.setHost(hostName)
									.setPort(port)
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
