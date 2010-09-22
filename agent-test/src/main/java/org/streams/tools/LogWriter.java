package org.streams.tools;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;

/**
 * 
 * Writes out test log files with automatic roll over on the minute.<br/>
 * The aim of this class is to simulate log file generation.
 */
public class LogWriter implements Runnable {

//	private static final Logger LOG = Logger.getLogger(LogWriter.class);
	
	public static final String LOGWRITER_DIR = "logwriter.dir";

	File logDir;

	List<File> files = new ArrayList<File>();

	AtomicBoolean shouldRun = new AtomicBoolean(true);

	public LogWriter(Configuration conf) {
		String logDirName = conf.getString(LOGWRITER_DIR, "/tmp/logwriter_logs");
		logDir = new File(logDirName);
		logDir.mkdirs();

		System.out.println("Using log direcotry: " + logDir);
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				shutdown();
			}
		});

	}

	public List<File> getFiles() {
		return files;
	}

	public void shutdown() {
		shouldRun.set(false);
	}

	public void run() {

		try {

			int counter = 0;
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-hh-mm");

			while (shouldRun.get()) {

				long currentTime = System.currentTimeMillis();
				File file = new File(logDir,
						format.format(new Date(currentTime)) + "." + counter
								+ ".log");
				file.createNewFile();

				files.add(file);

				FileWriter writer = new FileWriter(file);
				
				
				try {
					int i = 0;
					// write to file until a minute has passed
					while (System.currentTimeMillis() - currentTime < 60000
							&& shouldRun.get()) {
						writer.write("A_" + i + "\tB_" + i + "\n");
						i++;
					}
					
				} finally {
					writer.close();
				}

			}
		} catch (Throwable t) {
			t.printStackTrace();
			throw new RuntimeException(t);
		}
	}

	@SuppressWarnings("unchecked")
	public static void main(String arg[]) throws Exception {

		if (arg.length != 1) {
			throw new RuntimeException(
					"Please provide 1 argument path  : hadoop style configuration file");
		}

		String confFile = arg[0];

		System.out.println("Using configuration file " + confFile);
		
		PropertiesConfiguration props = new PropertiesConfiguration(new File(confFile));
		SystemConfiguration sys = new SystemConfiguration();
		CompositeConfiguration cc = new CompositeConfiguration();
		cc.addConfiguration(sys);
		cc.addConfiguration(props);

		Iterator<String> it = props.getKeys();

		while (it.hasNext()) {
			String key = it.next();
			System.setProperty(key, props.getProperty(key).toString());
		}
		
		LogWriter logWriter = new LogWriter(cc);
		logWriter.run();

	}

}
