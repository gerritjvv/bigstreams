package org.streams.agent.file.actions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collection;

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

}
