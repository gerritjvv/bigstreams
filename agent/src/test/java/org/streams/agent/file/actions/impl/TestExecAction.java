package org.streams.agent.file.actions.impl;

import java.io.File;
import java.io.IOException;

import org.junit.Test;
import org.streams.agent.file.FileTrackingStatus;

/**
 * 
 * Tests the move action
 * 
 */
public class TestExecAction {

	@Test(expected = RuntimeException.class)
	public void testExecError() throws IOException {

		File file = new File("target/tests/testExecAction/test.txt");
		file.getParentFile().mkdirs();
		file.createNewFile();
		
		String cmd = "dfkjdfjslfd";

		ExecAction action = new ExecAction();
		action.configure(cmd);
		action.setDelayInSeconds(1);
		action.setLogType("type1");
		action.setStatus(FileTrackingStatus.STATUS.DONE);

		FileTrackingStatus status = new FileTrackingStatus();
		status.setPath(file.getAbsolutePath());

		action.run(status);

	}

	@Test
	public void testExec() throws IOException {

		File file = new File("target/tests/testExecAction/test.txt");
		file.getParentFile().mkdirs();
		file.createNewFile();
		
		String cmd = "java";

		ExecAction action = new ExecAction();
		action.configure(cmd);
		action.setDelayInSeconds(1);
		action.setLogType("type1");
		action.setStatus(FileTrackingStatus.STATUS.DONE);

		FileTrackingStatus status = new FileTrackingStatus();
		status.setPath(file.getAbsolutePath());

		action.run(status);

	}

}
