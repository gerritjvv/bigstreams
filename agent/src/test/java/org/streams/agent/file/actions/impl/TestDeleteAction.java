package org.streams.agent.file.actions.impl;

import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.streams.agent.file.FileTrackingStatus;

/**
 * 
 * Tests the move action
 * 
 */
public class TestDeleteAction {

	static File baseDir;

	@Test
	public void testDelete() throws IOException {

		File file = new File(baseDir, "testMove.txt");
		file.createNewFile();

		DeleteAction action = new DeleteAction();
		action.setDelayInSeconds(1);
		action.setLogType("type1");
		action.setStatus(FileTrackingStatus.STATUS.DONE);

		/**
		 * public FileTrackingStatus(long lastModificationTime, long fileSize,
		 * String path, STATUS status, int linePointer, long filePointer, String
		 * logType, Date fileDate, Date sentDate) {
		 */
		FileTrackingStatus status = new FileTrackingStatus();
		status.setPath(file.getAbsolutePath());

		action.run(status);

		assertFalse(file.exists());

	}

	@BeforeClass
	public static void setup() {

		baseDir = new File("target/tests/testMoveAction");
		baseDir.mkdirs();

	}

	@AfterClass
	public static void shutdown() throws IOException {
		FileUtils.deleteDirectory(baseDir);
	}

}
