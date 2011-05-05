package org.streams.agent.file.actions.impl;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.streams.agent.conf.LogDirConf;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.actions.FileLogManageAction;
import org.streams.agent.file.actions.FileLogManageActionFactory;

/**
 * 
 * Default action manage factory.<br/>
 * 
 * [logtype] [status] [delay in seconds] [action name] [rest of config]\n
 */
public class DefaultFileLogManageActionFactory implements
		FileLogManageActionFactory {

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

	public DefaultFileLogManageActionFactory(LogDirConf logDirConf) {
		super();
		this.logDirConf = logDirConf;
	}

	@Override
	public FileLogManageAction create(String config) {

		String[] arr = config.split(" ");
		if (arr.length < 4) {
			throw new RuntimeException(
					"Bad configuration: format is [logtype] [status] [dealy in seconds] [action name] [action config]\n");
		}

		String logType = arr[0];
		
		//validate logType, if not exist print a warning
		boolean existLogType = false;
		Collection<String> logTypes = logDirConf.getTypes();
		existLogType = logTypes != null && logTypes.contains(logType);
		
		if(!existLogType){
			LOG.warn("Log type: " + logType + " is not present in the stream_directories file");
		}
		
		FileTrackingStatus.STATUS status = FileTrackingStatus.STATUS
				.valueOf(arr[1].toUpperCase());
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

		try {
			FileLogManageAction action = actionName.createInstance();
			action.setLogType(logType);
			action.setDelayInSeconds(delayInSeconds);
			action.setStatus(status);
			action.configure(actionConfig);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Creating action: " + actionName);
			}

			return action;
		} catch (Throwable t) {
			throw new RuntimeException("Error creating action " + actionName, t);
		}

	}

}
