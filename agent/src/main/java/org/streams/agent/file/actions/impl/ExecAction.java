package org.streams.agent.file.actions.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.actions.FileLogManageAction;

/**
 * 
 * Run an external command Accepts a command string. This string is split by
 * spaces and each space separated pair is passed to the ProcessBuilder and a
 * separate command item.
 * 
 * @TODO Complete sysout should be read by a thread
 */
public class ExecAction extends FileLogManageAction {

	private static final Logger LOG = Logger.getLogger(ExecAction.class);

	String cmd;

	@Override
	public void runAction(FileTrackingStatus fileStatus) throws Throwable {

		File file = new File(fileStatus.getPath());
		if (file.exists()) {
			List<String> cmdList = new ArrayList<String>();
			cmdList.addAll(Arrays.asList(cmd.split(" ")));

			Process process = new ProcessBuilder(cmdList).start();
			try {
				if (process.waitFor() != 0) {
					throw new IOException("Error running process " + cmd + " "
							+ file.getAbsolutePath());
				}
				
				if(LOG.isDebugEnabled()){
					LOG.debug("Run Exec: " + cmd + " " + file.getAbsolutePath());
				}
				
			} finally {
				process.destroy();
			}

		}

	}

	@Override
	public void configure(String config) {

		if (config == null || config.length() < 1) {
			throw new RuntimeException("Exec action must have a command");
		}

		cmd = config.trim();
	}

}
