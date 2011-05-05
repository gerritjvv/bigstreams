package org.streams.agent.file.actions;

/**
 * 
 * Default supported configuration pattern should be.:<br/>
 * [logtype] [status] [delay in seconds] [action name] [rest of config]\n
 *
 */
public interface FileLogManageActionFactory {

	/**
	 * Creates a FileLogManageAction instance from the configuration
	 * @param config String string configuration pattern
	 * @return FileLogManageAction
	 */
	FileLogManageAction create(String config);
	
}
