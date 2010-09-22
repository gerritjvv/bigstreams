package org.streams.agent.main;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.junit.Test;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.Restlet;
import org.restlet.resource.Finder;
import org.restlet.resource.ServerResource;
import org.restlet.routing.Router;
import org.streams.agent.cli.impl.StopAgent;
import org.streams.agent.conf.AgentProperties;
import org.streams.agent.main.Bootstrap;
import org.streams.agent.mon.impl.AgentShutdownResource;
import org.streams.commons.app.AppLifeCycleManager;
import org.streams.commons.cli.CommandLineParser;
import org.streams.commons.cli.CommandLineProcessor;
import org.streams.commons.cli.CommandLineProcessorFactory;


/**
 * Test the StopAgent implementation
 * 
 */
public class TestStopAgentCommand extends TestCase {


	/**
	 * 
	 * @throws Exception
	 */
	@Test
	public void testShutdown() throws Exception {

		Configuration conf = new MapConfiguration(new HashMap<String, Object>());
		conf.setProperty(AgentProperties.MONITORING_PORT, 5045);

		org.restlet.Client client = new org.restlet.Client(
				org.restlet.data.Protocol.HTTP);
		
		final StopAgent stopAgent = new StopAgent(client, conf);
		
		Bootstrap bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.REST_CLIENT);
		
		CommandLineParser parser = bootstrap.agentCommandLineParser(new CommandLineProcessorFactory(){

			@Override
			public CommandLineProcessor create(String name, PROFILE... profiles) {
				return stopAgent;
			}
			
		});
		
		//setup rest resource
		final AtomicBoolean isShutdown = new AtomicBoolean();
		final AgentShutdownResource shutdownResource = new AgentShutdownResource(new AppLifeCycleManager() {
			
			@Override
			public void shutdown() {
				isShutdown.set(true);
			}
			
			@Override
			public void kill() {
				
			}
			
			@Override
			public void init() throws Exception {
				
			}
		});
		
		final Finder shutdownResoureFinder = new Finder() {

			@Override
			public ServerResource find(Request request, Response response) {
				return shutdownResource;
			}

		};

		
		final Router router = new Router();
		router.attach("/agent/shutdown", shutdownResoureFinder);
				

		Application app = new Application() {

			@Override
			public Restlet createInboundRoot() {
				return router;
			}

		};

		//startup the rest component and listen on port 8040
		Component component = new Component();
		component.getServers().add(org.restlet.data.Protocol.HTTP, 5045);
		component.getDefaultHost().attach(app);
		component.start();
		
		//send the shutdown command via the parser
		try{
			parser.parse(System.out, new String[]{"-stop", "agent"});
		}finally{
			component.stop();
		}
		
		//all the above will lead to the ApplicationLifeCycleManager shutdown method being called.
		//here we check that this is true
		assertTrue(isShutdown.get());
	}


}
