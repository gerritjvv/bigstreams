package org.streams.commons.group;

import java.net.InetSocketAddress;
import java.util.List;

public interface GroupChangeListener {

	void groupChanged(List<InetSocketAddress> members);

}
