package org.streams.test.commons.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.streams.commons.group.Group.GroupStatus;
import org.streams.commons.group.GroupKeeper;
import org.streams.commons.zookeeper.ZConnection;
import org.streams.commons.zookeeper.ZGroup;

public class ZGroupIntegrationTest {

	ZGroup group;

	@Before
	public void before() throws IOException, InterruptedException, KeeperException {
		ZConnection.getInstance().reset();

		// create agent

		group = new ZGroup("localhost:3001", 10000L);
		GroupStatus agent = GroupStatus.newBuilder().setHost("localhost")
				.setLastUpdate(System.currentTimeMillis()).setLoad(10)
				.setMsg("TEST").setType(GroupStatus.Type.AGENT)
				.setStatus(GroupStatus.Status.OK).build();

		group.updateStatus(agent);

		GroupStatus collector = GroupStatus.newBuilder().setHost("localhost")
				.setLastUpdate(System.currentTimeMillis()).setLoad(10)
				.setMsg("TEST").setType(GroupStatus.Type.COLLECTOR)
				.setStatus(GroupStatus.Status.OK).build();

		group.updateStatus(collector);
	}

	@AfterClass
	public static void after() {
		ZConnection.getInstance().close();
	}

	@Test
	public void testGroupList() throws IOException, InterruptedException,
			KeeperException {

		//assert that the basedir exists
		ZooKeeper zk = ZConnection.getConnectedInstance("localhost:3001", 10000L);
		assertNotNull(zk.exists(ZGroup.BASEDIR, false));
		
		
		Collection<GroupKeeper.GROUPS> groups = group.listGroups();
		assertNotNull(groups);
		
		assertEquals(2, groups.size());
		assertTrue(groups.contains(GroupKeeper.GROUPS.AGENTS));
		assertTrue(groups.contains(GroupKeeper.GROUPS.COLLECTORS));
	}

	@Test
	public void testListChildren() throws IOException, InterruptedException,
			KeeperException {

		Collection<GroupStatus> groupStats = group.listStatus(GroupKeeper.GROUPS.AGENTS);
		assertNotNull(groupStats);
		assertEquals(1, groupStats.size());
		assertEquals(groupStats.iterator().next().getType(), GroupStatus.Type.AGENT);
		
		groupStats = group.listStatus(GroupKeeper.GROUPS.COLLECTORS);
		assertNotNull(groupStats);
		assertEquals(1, groupStats.size());
		assertEquals(groupStats.iterator().next().getType(), GroupStatus.Type.COLLECTOR);
	}
	
}
