package org.streams.agent.file.actions.impl;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.streams.agent.conf.LogDirConf;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.actions.FileLogManageAction;
import org.streams.agent.file.actions.FileLogManageActionFactory;

/**
 * 
 * Default action manage factory.<br/>
 * 
 * <pre>
 * [logtype] [status] [delay in seconds] [action name] [rest of config]\n
 * </pre>
 * 
 * <br/>
 * Wild cards:<br/>
 * Logtype, and status can have wild cards which means that they are notified
 * for each and every type and status.<br/>
 * E.g.<br/>
 * 
 * <pre>
 * &quot;*  mystatus 0 move /test/&quot;
 * </pre>
 * 
 * <br/>
 * The above example will move all logs with status "mystatus" to the directory
 * test. <br/>
 * 
 * <pre>
 * &quot;* READ_ERROR delete&quot;
 * </pre>
 * 
 * The above example will delete all logs that have status READ_ERROR. </pre>
 * </br>
 */
public class DefaultFileLogManageActionFactory implements
		FileLogManageActionFactory {

	private static final String skippCommentsRegex = "/\\*(?:.|[\\n\\r])*?\\*/";

	private static final Logger LOG = Logger
			.getLogger(DefaultFileLogManageActionFactory.class);

	enum ACTION_NAMES {
		EXEC(ExecAction.class), MOVE(MoveAction.class), DELETE(
				DeleteAction.class);

		Class<? extends FileLogManageAction> actionCls;

		ACTION_NAMES(Class<? extends FileLogManageAction> actionCls) {
			this.actionCls = actionCls;
		}

		public FileLogManageAction createInstance()
				throws InstantiationException, IllegalAccessException {
			return actionCls.newInstance();
		}

	}

	LogDirConf logDirConf;

	/**
	 * Create an instance passing the current LogDirConf object ot it.
	 * 
	 * @param logDirConf
	 */
	public DefaultFileLogManageActionFactory(LogDirConf logDirConf) {
		super();
		this.logDirConf = logDirConf;
	}

	@Override
	public Collection<FileLogManageAction> create(String config) {

		Collection<FileLogManageAction> coll = new ArrayList<FileLogManageAction>();
		
		String[] arr = config.split(" ");
		if (arr.length < 4) {
			throw new RuntimeException(
					"Bad configuration( " + config + "): format is [logtype] [status] [dealy in seconds] [action name] [action config]\n");
		}

		String logType = arr[0].trim();
		Collection<String> logTypesFound = new ArrayList<String>();
		
		// validate logType, if not exist print a warning
		boolean existLogType = false;
		Collection<String> logTypes = logDirConf.getTypes();
		
		if(logType.equals("*")){
			logTypesFound = logTypes;
		}else{
		
			existLogType = logTypes != null && logTypes.contains(logType);
	
			if (!existLogType) {
				LOG.warn("Log type: " + logType
						+ " is not present in the stream_directories file");
			}
			
			logTypesFound.add(logType);
		}
		
		FileTrackingStatus.STATUS status = null;
		Collection<FileTrackingStatus.STATUS> statusesFound = null;
		
		String statusStr = arr[1].trim();
		if(statusStr.equals("*")){
			statusesFound = Arrays.asList(FileTrackingStatus.STATUS.values());
		}else{
			
			try {
				status = FileTrackingStatus.STATUS.valueOf(arr[1].toUpperCase());
			} catch (Throwable t) {
				String str = arr[1]
						+ " was not recognised please provide a status with name "
						+ Arrays.asList(FileTrackingStatus.STATUS.values());
				throw new RuntimeException(str);
			}
			
			statusesFound = new ArrayList<FileTrackingStatus.STATUS>();
			statusesFound.add(status);
			
		}
		
		int delayInSeconds = Integer.parseInt(arr[2]);
		if (delayInSeconds < 0) {
			delayInSeconds = 1;
		}

		ACTION_NAMES actionName = ACTION_NAMES.valueOf(arr[3].toUpperCase());
		if (actionName == null) {
			throw new RuntimeException("The action name " + arr[3]
					+ " is not supported");
		}

		// re create action config into one string
		String actionConfig = null;
		if (arr.length > 4) {
			StringBuilder configBuilderStr = new StringBuilder();
			for (int i = 4; i < arr.length; i++) {
				if (i != 0)
					configBuilderStr.append(" ");

				configBuilderStr.append(arr[i]);
			}

			actionConfig = configBuilderStr.toString();
		}

		//for each log type for each status create an action
		for(String logTypeFound : logTypesFound){
			for(FileTrackingStatus.STATUS statusFound : statusesFound){
				try {
					FileLogManageAction action = actionName.createInstance();
					action.setLogType(logTypeFound);
					action.setDelayInSeconds(delayInSeconds);
					action.setStatus(statusFound);
					action.configure(actionConfig);
		
					if (LOG.isDebugEnabled()) {
						LOG.debug("Creating action: " + actionName);
					}
					
					coll.add(action);
				} catch (Throwable t) {
					throw new RuntimeException("Error creating action " + actionName, t);
				}
			}
		}
		
		return coll;
	}

	@Override
	public Collection<FileLogManageAction> create(Reader reader) {
		Collection<FileLogManageAction> coll = new ArrayList<FileLogManageAction>();

		String fileContents = null;
		try {
			fileContents = IOUtils.toString(reader);
		} catch (Throwable t) {
			IOUtils.closeQuietly(reader);
		}

		if (fileContents != null) {

			fileContents = fileContents.replaceAll(skippCommentsRegex, "");
			Scanner scanner = new Scanner(new StringReader(fileContents));
			try {
				while (scanner.hasNext()) {

					String line = scanner.nextLine().trim();
					if (line.length() < 1 || line.startsWith("#") || line.startsWith("/"))
						continue;

					coll.addAll(create(line));
				}

			} finally {
				scanner.close();
			}

		}

		return coll;
	}

}
