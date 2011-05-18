package org.streams.agent.file.actions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the configuration for the log manage actions
 * 
 */
public class LogActionsConf {

	Collection<? extends FileLogManageAction> actions;

	public LogActionsConf(FileLogManageActionFactory factory, File file)
			throws FileNotFoundException {

		actions = factory.create(new FileReader(file));

	}

	LogActionsConf(Collection<? extends FileLogManageAction> actions) {
		this.actions = actions;
	}

	public Collection<? extends FileLogManageAction> getActions() {
		return actions;
	}

	public void setActions(Collection<? extends FileLogManageAction> actions) {
		this.actions = actions;
	}

	public Map<String, Object> toMap() {

		Map<String, Object> map = new HashMap<String, Object>();

		if (actions != null) {
			int i = 1;
			for (FileLogManageAction action : actions) {
				map.put(String.valueOf(i++),
						action.getStatus() + " " + action.getLogType() + " "
								+ action.getDelayInSeconds() + " "
								+ action.getName());
			}
		}

		return map;
	}

}
