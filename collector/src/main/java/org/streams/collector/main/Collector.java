package org.streams.collector.main;

import java.lang.Thread.UncaughtExceptionHandler;

import org.streams.commons.cli.CommandLineParser;

/**
 * 
 * The Collector entry point. Starts the DI and retrieves the
 * AppLifeCycleManager.<br/>
 * <p/>
 */
public class Collector {

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
		CommandLineParser parser = bootstrap.commandLineParser();
		parser.parse(System.out, arg);

		Runtime.getRuntime().addShutdownHook(new Thread() {

			public void run() {
					try{
						bootstrap.close();
					}catch(Throwable t){
						t.printStackTrace();
					}
					System.out.println("Collector Shutdown");
			}
		});

	}
}
