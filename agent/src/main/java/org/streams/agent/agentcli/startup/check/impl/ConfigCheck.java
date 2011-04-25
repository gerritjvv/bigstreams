package org.streams.agent.agentcli.startup.check.impl;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.log4j.Logger;
import org.streams.agent.conf.AgentConfiguration;
import org.streams.agent.conf.LogDirConf;
import org.streams.commons.app.AbstractStartupCheck;

/**
 * 
 * Checks that the configuration can be loaded.<br/>
 * Also checks for the following consistencies in the stream_directories:<br/>
 * <ul>
 * <li>There are paths specified.</li>
 * <li>Each paths exists, is a directory and can be read.</li>
 * <li>Each path has a log type associated with it.</li>
 * <li>Each path is only specified once.</li>
 * </ul>
 * 
 */
@Named
public class ConfigCheck extends AbstractStartupCheck {

	private static final Logger LOG = Logger.getLogger(ConfigCheck.class);

	LogDirConf logDirConf = null;
	AgentConfiguration configuration;

	public ConfigCheck() {
	}

	public ConfigCheck(LogDirConf logDirConf, AgentConfiguration configuration) {
		this.logDirConf = logDirConf;
		this.configuration = configuration;
	}

	@Override
	public void runCheck() throws Exception {

		LOG.info("Checking Configuration");

		// check to LogDirConf
		checkTrue(logDirConf != null, "No LogDirConf instance provided");
		Collection<File> files = logDirConf.getDirectories();

		checkTrue(!(files == null || files.size() < 1),
				"No directories specified in stream_directories configuration file");

		Map<String, File> consistencyMap = new HashMap<String, File>();

		for (File file : files) {
			// see the javadoc above for a textual explanation of each check
			// performed here.
			String fileName = file.getName();
			File dirToCheck = file;

			if (fileName.contains("*") || fileName.contains("?")) {
				dirToCheck = file.getParentFile();
			}

			checkTrue(dirToCheck.exists(),
					"The directory " + dirToCheck.getAbsolutePath()
							+ " does not exist");
			checkTrue(dirToCheck.isDirectory(),
					"The path " + dirToCheck.getAbsolutePath()
							+ " must be a directory");
			checkTrue(dirToCheck.canRead(),
					"The path " + dirToCheck.getAbsolutePath()
							+ " is not readable");

			String logType = logDirConf.getLogType(file);

			checkTrue(logType != null, "The path " + file.getAbsolutePath()
					+ " does not have a log type associated with it");

			checkTrue(!consistencyMap.containsKey(file.getAbsolutePath()),
					"The path " + file.getAbsolutePath()
							+ " is specified more than once");

		}

		// check to general Configuration
		checkTrue(configuration != null,
				"No Configuration (streams-agent.properties) found");

		LOG.info("DONE");

	}

	public LogDirConf getLogDirConf() {
		return logDirConf;
	}

	@Inject
	public void setLogDirConf(LogDirConf logDirConf) {
		this.logDirConf = logDirConf;
	}

	public AgentConfiguration getConfiguration() {
		return configuration;
	}

	@Inject
	public void setConfiguration(AgentConfiguration configuration) {
		this.configuration = configuration;
	}

}
