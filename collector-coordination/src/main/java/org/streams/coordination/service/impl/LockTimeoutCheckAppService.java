package org.streams.coordination.service.impl;

import java.util.Timer;
import java.util.TimerTask;

import org.streams.commons.app.ApplicationService;
import org.streams.coordination.service.LockMemory;

/**
 * 
 * Creates a Timer that will periodically check for lock timeouts and remove them
 */
public class LockTimeoutCheckAppService implements ApplicationService{

	final long lockTimeoutCheckPeriod;
	final long lockTimeout;
	final LockMemory lockMemory;
	
	public LockTimeoutCheckAppService(long lockTimeoutCheckPeriod,
			long lockTimeout, LockMemory lockMemory) {
		super();
		this.lockTimeoutCheckPeriod = lockTimeoutCheckPeriod;
		this.lockTimeout = lockTimeout;
		this.lockMemory = lockMemory;
	}

	Timer timer;
	
	@Override
	public void start() throws Exception {
		if(timer != null){
			timer = new Timer("LockTimeoutCheckTimer");
			timer.schedule(new TimerTask() {
				
				@Override
				public void run() {
					try {
						lockMemory.removeTimedOutLocks(lockTimeout);
					} catch (InterruptedException e) {
						Thread.interrupted();
					}
				}
			}, 1000L, lockTimeoutCheckPeriod);
		}
		
	}

	@Override
	public void shutdown() {
		if(timer != null){
			timer.cancel();
		}
	}

	
	
}
