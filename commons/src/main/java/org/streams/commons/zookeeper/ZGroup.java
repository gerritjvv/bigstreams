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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
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
	ZConnection connection;
	String group;

	Map<GROUPS, List<GroupChangeListener>> addressSelectorMap = new ConcurrentHashMap<GroupKeeper.GROUPS, List<GroupChangeListener>>();

	/**
	 * Keep track of watchers registered.
	 */
	Map<GROUPS, GroupWatch> watcherMap = new ConcurrentHashMap<GroupKeeper.GROUPS, ZGroup.GroupWatch>();

	ScheduledExecutorService service = Executors
			.newSingleThreadScheduledExecutor();

	public ZGroup(ZConnection connection) throws KeeperException,
			InterruptedException, IOException {
		this("default", connection);
	}

	public ZGroup(final String group, final ZConnection connection)
			throws KeeperException, InterruptedException, IOException {
		this.connection = connection;
		this.group = group;

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
			final CuratorFramework zk = getZK();

			if (zk.checkExists().forPath(path) != null) {
				final List<String> children = zk.getChildren().forPath(path);
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
			final CuratorFramework zk = getZK();

			if (zk.checkExists().forPath(path) != null) {
				final List<String> children = zk.getChildren().forPath(path);
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

		final String path = BASEDIR + "/" + group;
		try {
			final CuratorFramework zk = getZK();

			if (zk.checkExists().forPath(path) != null) {

				final List<String> groupNames = zk.getChildren().forPath(path);
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
	public final void updateStatus(GroupStatus status) {

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
			ZPathUtil.store(getZK(), makePath(group + "/" + status.getHost()
					+ ":" + status.getPort()), status.toByteArray(), mode);
		} catch (Throwable t) {
			throw new GroupException(t);
		}

	}

	private final String makePath(String path) {
		return path.startsWith("/") ? BASEDIR + "/" + group + path : BASEDIR
				+ "/" + group + "/" + path;
	}

	private final CuratorFramework getZK() throws IOException,
			InterruptedException {
		return connection.get();
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
	private class GroupWatch implements BackgroundCallback {

		GROUPS groupType;
		String path;
		AtomicBoolean attached = new AtomicBoolean(false);

		public GroupWatch(GROUPS groupType) {
			this.groupType = groupType;
			path = makePath(groupType.toString());
		}

		public synchronized void attach() {

			try {
				CuratorFramework zk = getZK();

				zk.getChildren().watched().inBackground(this);

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
		public synchronized void processResult(CuratorFramework client,
				CuratorEvent event) throws Exception {
			if (event.getType().equals(CuratorEventType.CHILDREN)
					|| event.getType().equals(CuratorEventType.CREATE)
					|| event.getType().equals(CuratorEventType.DELETE)) {
				attach();
			}

		}

	}

}
