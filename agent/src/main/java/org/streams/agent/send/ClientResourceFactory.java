package org.streams.agent.send;

/**
 * 
 * Creates and manages ClientResourceInstances.<br/>
 * The resources like thread pools and timers that each ClientResourceInstance
 * uses is managed<br/>
 * by this factory class.<br/>
 * <p/>
 * Because this factory manages thread pools and timer instances it has a
 * lifecycle.<br/>
 * All resources are created and initialised on instantiation.<br/>
 * The resources are released again only when the destroy method is called.<br/>
 */
public interface ClientResourceFactory {

	/**
	 * Get a ClientResource instance.
	 * 
	 * @return
	 */
	ClientResource get();

	/**
	 * Will release all resources.<br/>
	 * The ClientResourceFactory instance will not be usable after this method
	 * call.
	 */
	void destroy();

}
