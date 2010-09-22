package org.streams.test.coordination.util;

import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.Restlet;
import org.restlet.resource.Finder;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;
import org.restlet.routing.Template;

/**
 * A restlet that will always return OK
 * 
 */
public class AlwaysOKRestlet extends ServerResource {

	@Get("txt")
	public String toString() {
		return "OK";
	}

	/**
	 * Creates a Component that will used this reasource to always return ok.
	 * 
	 * @param port
	 * @return
	 */
	public static Component createComponent(int port) {

		Finder finder = new Finder() {

			@Override
			public ServerResource find(Request request, Response response) {
				return new AlwaysOKRestlet();
			}

		};

		final Router router = new Router();
		router.attach("/", finder, Template.MODE_STARTS_WITH);
		
		Application app = new Application() {

			@Override
			public Restlet createInboundRoot() {
				return router;
			}

		};

		Component component = new Component();
		component.getServers().add(org.restlet.data.Protocol.HTTP, port);
		component.getDefaultHost().attach(app);

		return component;
	}

}
