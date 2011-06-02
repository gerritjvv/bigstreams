package org.streams.coordination.main;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.streams.commons.cli.CommandLineParser;

/**
 * 
 * The Coordination entry point. Starts the DI and retrieves the
 * AppLifeCycleManager.<br/>
 * <p/>
 */
public class Coordination {

	public static void main(String arg[]) throws Exception {

		Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {

			@Override
			public void uncaughtException(Thread t, Throwable e) {
				e.printStackTrace();
				Runtime.getRuntime().exit(-1);
				try {
					Thread.sleep(1000);
				} catch (Throwable iexcp) {
					;// ignore
				}
				Runtime.getRuntime().halt(-1);
			}
		});

		final Bootstrap bootstrap = new Bootstrap();

		final ExecutorService shutdownService = Executors.newSingleThreadExecutor();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {

			public void run() {
				try {
					shutdownService.submit(new Runnable(){
						public void run(){
							bootstrap.close();
						}
					});
					
					shutdownService.shutdown();
					//wait 5 seconds for shutdown
					shutdownService.awaitTermination(5000, TimeUnit.MILLISECONDS);
					shutdownService.shutdownNow();
					
				} catch (Throwable t) {
					t.printStackTrace();
				}
				System.out.println("Coordination Shutdown");
			}
		});

		CommandLineParser parser = bootstrap.commandLineParser();
		parser.parse(System.out, arg);

	}
}
