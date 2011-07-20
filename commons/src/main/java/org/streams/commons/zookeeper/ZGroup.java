package org.streams.commons.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.streams.commons.group.Group;
import org.streams.commons.group.Group.GroupStatus;
import org.streams.commons.group.GroupChangeListener;
import org.streams.commons.group.GroupException;
import org.streams.commons.group.GroupKeeper;

/**
 * 
 * Use zookeeper to implement group management. Two groups are supported. Agents
 * and Collector.<br/>
 * Agents status information is persistent while collector status information is
 * ephemeral.
 * 
 */
public class ZGroup implements GroupKeeper {

	private static final Logger LOG = Logger.getLogger(ZGroup.class);

	public static final String BASEDIR = "/streams_groups";
	final String hosts;
	final long timeout;
	String group;

	Map<GROUPS, List<GroupChangeListener>> addressSelectorMap = new ConcurrentHashMap<GroupKeeper.GROUPS, List<GroupChangeListener>>();

	/**
	 * Keep track of watchers registered.
	 */
	Map<GROUPS, GroupWatch> watcherMap = new ConcurrentHashMap<GroupKeeper.GROUPS, ZGroup.GroupWatch>();

	ScheduledExecutorService service = Executors
			.newSingleThreadScheduledExecutor();

	public ZGroup(String hosts, long timeout) throws KeeperException,
			InterruptedException, IOException {
		this("default", hosts, timeout);
	}

	public ZGroup(final String group, final String hosts, final long timeout)
			throws KeeperException, InterruptedException, IOException {
		this.hosts = hosts;
		this.timeout = timeout;
		this.group = group;
		ZPathUtil.mkdirs(ZConnection.getConnectedInstance(hosts, timeout),
				BASEDIR);

		// register thread that will attach watchers if not attached
		service.scheduleWithFixedDelay(new Runnable() {

			public void run() {

				if (watcherMap.size() > 0) {
					for (GroupWatch groupWatch : watcherMap.values()) {
						if (!groupWatch.attached.get()) {
							// this method must not throw any exception here
							groupWatch.attach();
						}
					}
				}

			}

		}, 10, 30, TimeUnit.SECONDS);
	}

	/**
	 * 
	 */
	@Override
	public List<InetSocketAddress> listMembers(GROUPS groupName) {

		String path = makePath(groupName.toString());

		List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();

		try {
			ZooKeeper zk = getZK();

			if (zk.exists(path, null) != null) {
				List<String> children = getZK().getChildren(path, null);
				if (children != null) {
					for (String childName : children) {
						// get data
						try {
							String split[] = childName.split(":");
							String host = split[0];
							int port = Integer.parseInt(split[1]);
							list.add(new InetSocketAddress(host, port));
						} catch (Throwable t) {
							LOG.error("Cannot parse member name: " + childName
									+ " for group " + path + " error: "
									+ t.toString());
						}
					}
				}
			}

		} catch (Throwable t) {
			throw new GroupException(t);
		}

		return list;
	}

	/**
	 * 
	 */
	@Override
	public Collection<GroupStatus> listStatus(GROUPS groupName) {

		String path = makePath(groupName.toString());

		Collection<GroupStatus> list = new ArrayList<GroupStatus>();

		try {
			ZooKeeper zk = getZK();

			if (zk.exists(path, null) != null) {
				List<String> children = getZK().getChildren(path, null);
				if (children != null) {
					for (String childName : children) {
						// get data

						byte data[] = ZPathUtil.get(zk, path + "/" + childName);

						if (data != null) {
							list.add(Group.GroupStatus.newBuilder()
									.mergeFrom(data).build());
						}

					}
				}
			}

		} catch (Throwable t) {
			throw new GroupException(t);
		}

		return list;
	}

	@Override
	public Collection<GROUPS> listGroups() {

		Collection<GROUPS> groups = null;

		String path = BASEDIR + "/" + group;
		try {
			ZooKeeper zk = getZK();

			if (zk.exists(path, null) != null) {

				List<String> groupNames = getZK().getChildren(path, false);
				groups = new ArrayList<GroupKeeper.GROUPS>(groupNames.size());
				for (String groupName : groupNames) {
					try {
						groups.add(GROUPS.valueOf(groupName.toUpperCase()));
					} catch (Throwable t) {
						LOG.error("Error reading group " + groupName);
						LOG.error(t.toString(), t);
					}
				}
			}

		} catch (Throwable t) {
			throw new GroupException(t);
		}

		return groups;
	}

	@Override
	public void updateStatus(GroupStatus status) {

		// collectors are ephemeral, we wants agent data to stay live even after
		// its down
		CreateMode mode = null;
		String group = null;

		if (status.getType().equals(GroupStatus.Type.COLLECTOR)) {

			mode = CreateMode.EPHEMERAL;
			group = GROUPS.COLLECTORS.toString();
		} else {
			mode = CreateMode.PERSISTENT;
			group = GROUPS.AGENTS.toString();
		}

		try {

			byte data[] = status.toByteArray();

			ZooKeeper zk = getZK();

			String path = makePath(group + "/" + status.getHost() + ":"
					+ status.getPort());

			// ensure that the path has been created
			// at any time external code can remove the nodes, here we ensure
			// its created
			// before updating
			ZPathUtil.mkdirs(ZConnection.getConnectedInstance(hosts, timeout),
					path, mode);

			ZPathUtil.store(zk, path, data, mode);

		} catch (Throwable t) {
			throw new GroupException(t);
		}

	}

	private String makePath(String path) {
		return path.startsWith("/") ? BASEDIR + "/" + group + path : BASEDIR
				+ "/" + group + "/" + path;
	}

	private ZooKeeper getZK() throws IOException, InterruptedException {
		return ZConnection.getConnectedInstance(hosts, timeout);
	}

	@Override
	public void registerAddressSelector(GROUPS groupType,
			GroupChangeListener listener) {

		if (!watcherMap.containsKey(groupType)) {
			// the watcher map is only registered once
			GroupWatch groupWatch = new GroupWatch(groupType);
			groupWatch.attach();

			watcherMap.put(groupType, groupWatch);

		}

		List<GroupChangeListener> selectors = addressSelectorMap.get(groupType);

		if (selectors == null) {
			selectors = new ArrayList<GroupChangeListener>();
			addressSelectorMap.put(groupType, selectors);
		}
		selectors.add(listener);

	}

	/**
	 * Watcher that reattach itself on each event.<br/>
	 * Keeps track of group members.
	 */
	private class GroupWatch implements Watcher {

		GROUPS groupType;
		String path;
		AtomicBoolean attached = new AtomicBoolean(false);

		public GroupWatch(GROUPS groupType) {
			this.groupType = groupType;
			path = makePath(groupType.toString());
		}

		public synchronized void attach() {

			try {
				ZooKeeper zk = getZK();

				// reatach watch
				zk.getChildren(path, this);

				// get new group members
				List<GroupChangeListener> selectors = addressSelectorMap
						.get(groupType);

				List<InetSocketAddress> members = listMembers(groupType);

				LOG.info("Children Found: " + members);
				if (selectors != null) {
					for (GroupChangeListener listener : selectors) {
						listener.groupChanged(members);
					}
				}

				LOG.info("Watching " + path);
				attached.set(true);
			} catch (Throwable t) {
				// indicate to group register watcher thread to re se the
				// watcher
				attached.set(false);

				LOG.error(t.toString(), t);
			}

		}

		@Override
		public synchronized void process(WatchedEvent event) {
			LOG.info("Watch Event: " + event);

			if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged
					|| event.getType() == Watcher.Event.EventType.NodeCreated
					|| event.getType() == Watcher.Event.EventType.NodeDeleted) {

				LOG.info("NodeChildrenChanged Event: " + event);
				attach();

			}
		}

	}

}
