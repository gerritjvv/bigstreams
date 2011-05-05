package org.streams.agent.file.actions.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
public class TestMoveAction {

	static File baseDir;
	static File toMoveDir;

	@Test(expected = RuntimeException.class)
	public void testNotADirectoryError() throws IOException {

		File file = new File(baseDir, "testNotADirectoryError.txt");
		file.createNewFile();

		MoveAction action = new MoveAction();
		// expect exception
		action.configure(file.getAbsolutePath());
	}

	@Test(expected = RuntimeException.class)
	public void testCannotWrite() throws IOException {

		File file = new File(baseDir, "notwritedir");
		file.mkdirs();
		file.setWritable(false);
		try {
			MoveAction action = new MoveAction();
			// expect exception
			action.configure(file.getAbsolutePath());
		} finally {
			file.setWritable(true);
		}
	}

	@Test
	public void testMove() throws IOException {

		File file = new File(baseDir, "testMove.txt");
		file.createNewFile();

		File expectedFile = new File(toMoveDir, "testMove.txt");

		MoveAction action = new MoveAction();
		action.configure(toMoveDir.getAbsolutePath());
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
		assertTrue(expectedFile.exists());

	}

	@BeforeClass
	public static void setup() {

		baseDir = new File("target/tests/testMoveAction");
		baseDir.mkdirs();

		toMoveDir = new File(baseDir, "dir2");
		toMoveDir.mkdirs();

	}

	@AfterClass
	public static void shutdown() throws IOException {
		FileUtils.deleteDirectory(baseDir);
	}

}
