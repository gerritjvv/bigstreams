package org.streams.agent.main;

import java.lang.Thread.UncaughtExceptionHandler;

import org.slf4j.bridge.SLF4JBridgeHandler;
import org.streams.commons.cli.CommandLineParser;


/**
 * 
 * The Agent entry point. Starts the DI and retrieves the AppLifeCycleManager.<br/>
 * <p/>
 */
public class Main {

	public static void main(String arg[]) throws Exception {

		//set logging to log4j.
		SLF4JBridgeHandler.install();
		
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

		Bootstrap bootstrap = new Bootstrap();
		CommandLineParser parser = bootstrap.agentCommandLineParser();
		parser.parse(System.out, arg);
	}

}
