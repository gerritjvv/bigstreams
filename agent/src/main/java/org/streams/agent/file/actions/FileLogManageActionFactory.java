package org.streams.agent.file.actions;

import java.io.Reader;
import java.util.Collection;

/**
 * 
 * Default supported configuration pattern should be.:<br/>
 * [logtype] [status] [delay in seconds] [action name] [rest of config]\n
 *
 */
public interface FileLogManageActionFactory {

	/**
	 * Creates a FileLogManageAction instance from the configuration
	 * @param config String string configuration pattern This must be a single line.
	 * @return Collection of FileLogManageAction if wild cards are used for log type and status multiple actions are returned.
	 */
	Collection<FileLogManageAction> create(String config);
	
	/**
	 * Create from the reader
	 * @param reader
	 * @return Collection of FileLogManageAction
 	 */
	Collection<FileLogManageAction> create(Reader reader);
	
}
