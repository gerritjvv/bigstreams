package org.streams.coordinationtest;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.streams.commons.file.CoordinationServiceClient;
import org.streams.commons.file.FileTrackingStatus;
import org.streams.commons.file.SyncPointer;
import org.streams.commons.file.impl.CoordinationServiceClientImpl;
import org.streams.coordination.CoordinationProperties;


/**
 * Tests the service by Locking a huge amount of files 500000 and un locking the
 * same files times.
 * 
 */
public class TestMultiFileLock {

	private static Collection<SyncPointer> syncPointers = new ArrayList<SyncPointer>();

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = appConfig();

		int lockport = conf.getInt(
				CoordinationProperties.PROP.COORDINATION_LOCK_PORT.toString(),
				(Integer) CoordinationProperties.PROP.COORDINATION_LOCK_PORT
						.getDefaultValue());

		int unlockport = conf
				.getInt(CoordinationProperties.PROP.COORDINATION_UNLOCK_PORT
						.toString(),
						(Integer) CoordinationProperties.PROP.COORDINATION_UNLOCK_PORT
						.getDefaultValue());

		String hostname = "localhost";

		CoordinationServiceClient client = new CoordinationServiceClientImpl(
				new InetSocketAddress(hostname, lockport),
				new InetSocketAddress(hostname, unlockport));

		System.out.println("Using url: " + hostname + " lockport: " + lockport
				+ " unlockport: " + unlockport);
		long maxTime = 0L;
		long minTime = Long.MAX_VALUE;
		int testCount = conf.getInt("tests.count", 1000);
		long total = 0L;

		for (int i = 0; i < testCount; i++) {
			long start = System.currentTimeMillis();

			lockFile(client, i);

			long diff = System.currentTimeMillis() - start;

			total += diff;
			maxTime = Math.max(maxTime, diff);
			minTime = Math.min(minTime, diff);

		}

		System.out
				.println("----------------- LOCK Test Multi File Locks Results ---------------------- ");
		System.out.println("Tests run: " + testCount);
		System.out.println("MaxTime: " + maxTime);
		System.out.println("MinTime: " + minTime);
		System.out.println("AvgTime: " + (total / testCount));
		System.out
				.println("----------------- ----------------------------- ---------------------- ");

		for (SyncPointer syncPointer : syncPointers) {
			long start = System.currentTimeMillis();
			releaseFile(client, syncPointer);
			long diff = System.currentTimeMillis() - start;

			total += diff;
			maxTime = Math.max(maxTime, diff);
			minTime = Math.min(minTime, diff);

		}

		System.out
				.println("-----------------RELEASE Test Multi File Locks Results ---------------------- ");
		System.out.println("Tests run: " + testCount);
		System.out.println("MaxTime: " + maxTime);
		System.out.println("MinTime: " + minTime);
		System.out.println("AvgTime: " + (total / testCount));
		System.out
				.println("----------------- ----------------------------- ---------------------- ");

	}

	private static void releaseFile(CoordinationServiceClient client,
			SyncPointer syncPointer) {

		client.saveAndFreeLock(syncPointer);
	}

	/**
	 * Send a lock file request and a release file request
	 * 
	 * @param url
	 */
	private static void lockFile(CoordinationServiceClient client, int i) {

		FileTrackingStatus fileStatus = new FileTrackingStatus(0L, 10L, 0,
				"test1_agent" + i, "file" + i, "type" + i);

		SyncPointer syncPointer = client.getAndLock(fileStatus);

		assert syncPointer != null;

		syncPointers.add(syncPointer);

	}

	public static org.apache.commons.configuration.Configuration appConfig() {

		URL url = Thread.currentThread().getContextClassLoader()
				.getResource("conf.properties");
		if (url == null) {
			throw new RuntimeException(
					"cant find configuration streams-coordination.properties");
		}

		CombinedConfiguration cc = new CombinedConfiguration();

		PropertiesConfiguration props;
		try {
			props = new PropertiesConfiguration(url);
			cc.addConfiguration(props);
		} catch (ConfigurationException e) {
			RuntimeException rte = new RuntimeException(e);
			rte.setStackTrace(e.getStackTrace());
			throw rte;
		}

		return cc;
	}

}
