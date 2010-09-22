package org.streams.commons.app;

/**
 * 
 * on application startup, several checks are run. each check can stop the
 * startup cycle by throwing an exception. StartupCheck(s) are devided into 2
 * categories.<br/>
 * <ul>
 * <li>Pre startup -> run before services are started</li>
 * <li>Post startup -> run after application startup and all services are
 * started</li>
 * </ul>
 */
public interface StartupCheck {

	/**
	 * If an exception is thrown the application is shutdown.
	 * 
	 * @throws Exception
	 */
	public void runCheck() throws Exception;

}
