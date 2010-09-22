package org.streams.commons.app;

/**
 * Provide helper methods to help writing StartupChecks.
 *
 */
public abstract class AbstractStartupCheck implements StartupCheck{

	/**
	 * If the expression is not true a RuntimeException is thrown with the msg provided.
	 * @param expression
	 * @param msg
	 */
	protected void checkTrue(boolean expression, String msg){
		if(!expression){
			throw new RuntimeException(msg);
		}
	}
}
