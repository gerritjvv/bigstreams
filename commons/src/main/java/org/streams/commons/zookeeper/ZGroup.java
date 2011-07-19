package org.streams.commons.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.streams.commons.group.Group;
import org.streams.commons.group.Group.GroupStatus;
import org.streams.commons.group.GroupException;
import org.streams.commons.group.GroupKeeper;

/**
 * 
 * Use zookeeper to implement group management.
 * Two groups are supported. Agents and Collector.<br/>
 * Agents status information is persistent while collector status information is ephemeral.
 *
 */
public class ZGroup implements GroupKeeper{

	private static final Logger LOG = Logger.getLogger(ZGroup.class);
	
	public static final String BASEDIR = "/streams_groups";
	String hosts;
	long timeout;
	String group;
	
	public ZGroup(String hosts, long timeout) throws KeeperException, InterruptedException, IOException{
		this("default", hosts, timeout);
	}
	
	
	public ZGroup(String group, String hosts, long timeout) throws KeeperException, InterruptedException, IOException{
		this.hosts = hosts;
		this.timeout = timeout;
		this.group = group;
		ZPathUtil.mkdirs(ZConnection.getConnectedInstance(hosts, timeout), BASEDIR);
		
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
			
			if(zk.exists(path, null) != null){
				List<String> children = getZK().getChildren(path, null);
				if(children != null){
					for(String childName: children){
						//get data
						
						byte data[] = ZPathUtil.get(zk, path + "/" + childName);
						
						if(data != null){
							list.add( Group.GroupStatus.newBuilder().mergeFrom(data).build() );
						}
						
					}
				}
			}
			
		} catch (Throwable t){
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
			
			if(zk.exists(path, null) != null){
				
				List<String> groupNames = getZK().getChildren(path, false);
				groups = new ArrayList<GroupKeeper.GROUPS>(groupNames.size());
				for(String groupName : groupNames){
					try{
					groups.add(GROUPS.valueOf(groupName.toUpperCase()));
					}catch(Throwable t){
						LOG.error("Error reading group " + groupName);
						LOG.error(t.toString(), t);
					}
				}
			}
			
		} catch (Throwable t){
			throw new GroupException(t);
		}
		
		return groups;
	}

	@Override
	public void updateStatus(GroupStatus status) {
		
		//collectors are ephemeral, we wants agent data to stay live even after its down
		CreateMode mode = null;
		String group = null;
		
		 if(status.getType().equals(GroupStatus.Type.COLLECTOR)){
			 mode = CreateMode.EPHEMERAL;
			 group = GROUPS.COLLECTORS.toString();
		 }else{
			 mode = CreateMode.PERSISTENT;
			 group = GROUPS.AGENTS.toString();
		 }
		
		try {
			
			byte data[] = status.toByteArray();
			
			ZooKeeper zk = getZK();
			
			String path = makePath(group + "/" + status.getHost());
			
			//ensure that the path has been created
			//at any time external code can remove the nodes, here we ensure its created
			//before updating
			ZPathUtil.mkdirs(ZConnection.getConnectedInstance(hosts, timeout), path);
			
			ZPathUtil.store(zk, path, data, mode);
			
		} catch (Throwable t){
			throw new GroupException(t);
		}
		
		
	}

	private String makePath(String path){
		return path.startsWith("/") ? BASEDIR + "/" + group + path : BASEDIR + "/" + group + "/" + path;
	}
	
	private ZooKeeper getZK() throws IOException, InterruptedException{
		return ZConnection.getConnectedInstance(hosts, timeout);
	}
	
}
