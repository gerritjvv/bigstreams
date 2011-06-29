package org.streams.commons.cli;

import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.streams.commons.app.AppLifeCycleManager;
import org.streams.commons.cli.CommandLineProcessor;


/**
 * 
 * Implements the logic needed for staring the Application shutdown:<br/>
 * A SthudownHook is added to the runtime such that kill <pid> on the
 * application instance will result in a call to the shutdown method of the
 * AppLifeCycleManager instance, and on return of the method Runtime halt(0) is
 * called.
 */
public class AppStartCommand implements CommandLineProcessor {

	AppLifeCycleManager appLifeCycleManager;
	ExecutorService shutdownService = Executors.newFixedThreadPool(1);
	AtomicBoolean isShutdown = new AtomicBoolean(false);
	
	@Override
	public void process(CommandLine cmdLine, OutputStream out) throws Exception {

		// add shutdown hook.
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				shutdown();
			}
		});

		appLifeCycleManager.init();

	}

	private void shutdown() {
		//only shutdown once
		if(isShutdown.getAndSet(true)) return;
		
		shutdownService.submit(new Runnable(){
			public void run(){
				try{
				appLifeCycleManager.shutdown();
				}catch(Throwable t){
					t.printStackTrace();
				}
			}
		});
		shutdownService.shutdown();
		try {
			shutdownService.awaitTermination(15, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			//ignore we are shutting down
		}
		shutdownService.shutdownNow();
		
		Runtime.getRuntime().halt(0);
	}

	public AppLifeCycleManager getAppLifeCycleManager() {
		return appLifeCycleManager;
	}

	public void setAppLifeCycleManager(AppLifeCycleManager appLifeCycleManager) {
		this.appLifeCycleManager = appLifeCycleManager;
	}
}
