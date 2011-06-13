package org.streams.commons.util;

import org.jboss.netty.util.HashedWheelTimer;

/**
 * 
 * Singleton reference to the HashedWheelTimer.
 * 
 */
public class HashedWheelTimerFactory {

	private static final HashedWheelTimer timer = new HashedWheelTimer();
	
	static {
		timer.start();
	}

	public static final HashedWheelTimer getInstance() {
		return timer;
	}


	public static void shutdown(){
		timer.stop();
	}
	
}
