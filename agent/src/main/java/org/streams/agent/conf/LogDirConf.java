package org.streams.agent.conf;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * 
 * This class is tasked with loading the logDirConf file.<br/>
 * It supports reading bash style comments and single and multiline line java
 * comments.<br/>
 * The format supported is:<br/>
 * <logtype> <directory absolute path plus glob filter>
 * <p/>
 * One logical restriction is that only one logtype can be assigned to a
 * directory.<br/>
 * This makes sense that we shouldn't and in some applications its fatal to load
 * the same files twice each with a different logtype.
 */
public class LogDirConf {

	private static final Logger LOG = Logger.getLogger(LogDirConf.class);

	private static final String skippCommentsRegex = "/\\*(?:.|[\\n\\r])*?\\*/";

	Map<File, String> logDirs = new HashMap<File, String>();

	public LogDirConf(String fileName) throws IOException {
		this(new FileReader(fileName));
	}

	/**
	 * Load and parse the configuration file.<br/>
	 * The constructor checks that each directory is a directory, readable and
	 * exists.
	 */
	public LogDirConf(Reader fileReader) throws IOException {

		String fileContents = null;

		//

		try {
			fileContents = IOUtils.toString(fileReader);
		} catch (Throwable t) {
			IOUtils.closeQuietly(fileReader);
		}

		if (fileContents == null)
			throw new RuntimeException("The file is empty");

		fileContents = fileContents.replaceAll(skippCommentsRegex, "");

		Scanner scanner = new Scanner(new StringReader(fileContents));
		try {
			while (scanner.hasNext()) {

				String line = scanner.nextLine().trim();
				if (line.length() < 1 || line.startsWith("#"))
					continue;

				String[] split = line.split(" ");

				if (split == null || split.length != 2) {
					throw new RuntimeException(
							"The file  is not valid please use the format <logtype> <directory>");
				}

				String logType = split[0];
				String directoryPath = split[1];

				if (logType == null || directoryPath == null) {
					throw new RuntimeException(
					"The file is not valid, please use the format <logtype> <directory>");
				}

				
				File globDir = new File(directoryPath);
				
				//check for wild cards in the path
				//if any exist we can only check the parent directory.
				//e.g. /listfile/*.* can only check /listfile/
				String name = globDir.getName();
				File dir = globDir;
				if(name.contains("*") || name.contains("?")){
					dir = globDir.getParentFile();
				}
				
				if (!(dir.exists() && dir.isDirectory() && dir.canRead())) {
					throw new RuntimeException("The location " + dir.getAbsolutePath()
							+ " must exist and be a readable directory");
				}

				if (logDirs.containsKey(dir)) {
					throw new RuntimeException("Duplicate directory entry "
							+ dir + " in file ");
				}

				logDirs.put(globDir, logType);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Register directory " + globDir.getAbsolutePath()
							+ " logType: " + logType);
				}
			}

		} finally {
			scanner.close();
		}
	}

	/**
	 * Returns a Collection of Files that are the directories to be monitored
	 * for log files
	 * 
	 * @return
	 */
	public Collection<File> getDirectories() {
		return logDirs.keySet();
	}
	
	/**
	 * Returns the log types
	 * @return
	 */
	public Collection<String> getTypes(){
		return logDirs.values();
	}
	
	/**
	 * Returns the log type for a file
	 * 
	 * @param file
	 * @return
	 */
	public String getLogType(File file) {
		return logDirs.get(file);
	}

}
