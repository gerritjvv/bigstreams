package org.streams.collector.cli.impl;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.streams.commons.cli.CommandLineProcessor;
import org.streams.commons.cli.CommandLineProcessorFactory;
import org.streams.commons.cli.CommandLineProcessorFactory.PROFILE;


/**
 * Provides a command line interface with basic commands to view and manipulate
 * the FileTrackingStatus.<br/>
 * Commands:<br/>
 * <ul>
 * <li>help</li>
 * <li>ls [-agent]</li>
 * <li>count</li>
 * <li>start [collector]</li>
 * <li>stop [collector]</li>
 * <li>statusCollector</li>
 * </ul>
 * 
 */
public class CollectorCommandLineParser implements
		org.streams.commons.cli.CommandLineParser {

	CommandLineProcessorFactory factory;

	public CollectorCommandLineParser(CommandLineProcessorFactory factory) {
		super();
		this.factory = factory;
	}

	@Override
	public CommandLineProcessor parse(OutputStream out, String[] arguments)
			throws Exception {

		CommandLineParser parser = new GnuParser();

		CommandLine line = parser.parse(buildOptions(), arguments);

		CommandLineProcessor processor = null;

		boolean isOffline = line.hasOption("o");

		// command line processor profiles
		List<PROFILE> profiles = new ArrayList<PROFILE>();
		String processorName = null;

		if (line.hasOption("start")) {
			String value = line.getOptionValue("start");

			if (value.equalsIgnoreCase("collector")) {

				processorName = "startCollector";
				profiles.add(PROFILE.DB);
				profiles.add(PROFILE.REST_CLIENT);
				profiles.add(PROFILE.COLLECTOR);

			} else {
				throw new RuntimeException(value + " not recougnised");
			}

		} else if (line.hasOption("stop")) {

			String value = line.getOptionValue("stop");

			if (value.equalsIgnoreCase("collector")) {
				processorName = "stopCollector";
				profiles.add(PROFILE.REST_CLIENT);
				profiles.add(PROFILE.CLI);
			} else {
				throw new RuntimeException(value + " not recougnised");
			}
		} else {

			// these are all cli profiles (Note that startAgent is not
			// considered a normal CLI
			if (isOffline) {
				// if offline add the DB profile this means that the agent is
				// not running
				// and the cli should access the database directly.
				profiles.add(PROFILE.DB);
			} else {
				// this means that the agent is running and that the cli should
				// not access
				// the database directly but ratcher request information via the
				// agen rest interface.
				profiles.add(PROFILE.REST_CLIENT);
			}

			profiles.add(PROFILE.CLI);

			if (line.hasOption("collectorStatus")) {
				processorName = "collectorStatusCommand";
			} else {

				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("streams.sh", buildOptions());
				return null;
			}

		}

		// run command processor
		// note that we don't check for null here because the getProcessor
		// method throws an exception if not processor found.
		getProcessor(processorName, profiles.toArray(new PROFILE[] {}))
				.process(line, out);

		return processor;
	}

	/**
	 * Either returns a CommandLineProcessor or throws an error
	 * 
	 * @param key
	 * @return
	 */
	private CommandLineProcessor getProcessor(String name, PROFILE... profiles) {

		CommandLineProcessor p = factory.create(name, profiles);

		if (p == null) {
			throw new RuntimeException("Could not find any processor for "
					+ name);
		}

		return p;
	}

	/**
	 * Build command line options
	 * 
	 * @return
	 */
	@SuppressWarnings("static-access")
	private Options buildOptions() {

		Option help = new Option("help", "");

		Option start = OptionBuilder
				.hasOptionalArg()
				.withArgName("command")
				.withDescription(
						"e.g. collector will start the collector service")
				.create("start");

		Option stop = OptionBuilder.hasOptionalArg().withArgName("command")
				.withDescription("e.g. collector will stop the collect")
				.create("stop");

		Option json = OptionBuilder.withDescription("prints to json format ")
				.create("json");

		Option coordinationStatus = OptionBuilder.create("collectorStatus");

		Options options = new Options();
		options.addOption(help);
		options.addOption(start);
		options.addOption(stop);
		options.addOption(json);
		options.addOption(coordinationStatus);

		return options;
	}

	@Override
	public CommandLineProcessorFactory getFactory() {
		return factory;
	}

	@Override
	public void setFactory(CommandLineProcessorFactory factory) {
		this.factory = factory;
	}

}
