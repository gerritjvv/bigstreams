package org.streams.commons.zookeeper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.streams.commons.app.ApplicationService;

/**
 * 
 * Deletes data from a zstore that expired
 *
 */
public class ZStoreExpireCheckService implements ApplicationService{
	
	private static final Logger LOG = Logger.getLogger(ZStoreExpireCheckService.class);
	
	long initialDelay = 1000L;
	long checkFrequency = 60000;
	
	int dataTimeOut = 3600;
	
	List<ZStore> stores;
	
	ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

	public ZStoreExpireCheckService(){
		this.stores = new ArrayList<ZStore>();
	}
	
	
	public ZStoreExpireCheckService(List<ZStore> stores){
		this.stores = stores;
	}
	
	
	@Override
	public void start() throws Exception {
	
		LOG.info("Start ZStoreExpireCheckSerice with: intialDelay: "  + initialDelay + " checkFrequency: " + checkFrequency + " dataTimeout: " + dataTimeOut);
		
		service.scheduleWithFixedDelay(new Runnable(){
			
			public void run(){
				
				if(stores == null) return;
				int i = 0;
				for(ZStore store : stores){
					try{
						LOG.info("Checking store " + (i++) + " of " + stores.size());
					  	store.removeExpired(dataTimeOut);
					  	
					}catch(Throwable t){
						LOG.error(t);
					}
				}
				
			}
			
		}, initialDelay, checkFrequency, TimeUnit.MILLISECONDS);
		
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


	public long getInitialDelay() {
		return initialDelay;
	}


	public void setInitialDelay(long initialDelay) {
		this.initialDelay = initialDelay;
	}


	public long getCheckFrequency() {
		return checkFrequency;
	}


	public void setCheckFrequency(long checkFrequency) {
		this.checkFrequency = checkFrequency;
	}


	public int getDataTimeOut() {
		return dataTimeOut;
	}


	public void setDataTimeOut(int dataTimeOut) {
		this.dataTimeOut = dataTimeOut;
	}


	public List<ZStore> getStores() {
		return stores;
	}


	public void setStores(List<ZStore> stores) {
		this.stores = stores;
	}
	
 
	

}
