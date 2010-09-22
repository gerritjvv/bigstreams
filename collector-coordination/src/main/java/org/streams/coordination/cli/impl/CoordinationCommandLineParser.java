package org.streams.coordination.cli.impl;

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
 * <li>start [coordination]</li>
 * <li>stop [coordination]</li>
 * <li>statusCoordination</li>
 * </ul>
 * 
 */
public class CoordinationCommandLineParser implements org.streams.commons.cli.CommandLineParser {

	CommandLineProcessorFactory factory;

	public CoordinationCommandLineParser(CommandLineProcessorFactory factory) {
		super();
		this.factory = factory;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.specificmedia.hadoop.streams.agent.cli.impl.CommnadLineParser#parse
	 * (java.io.OutputStream, java.lang.String[])
	 */
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

			if (value.equalsIgnoreCase("coordination")) {

				processorName = "startCoordination";
				profiles.add(PROFILE.DB);
				profiles.add(PROFILE.REST_CLIENT);
				profiles.add(PROFILE.COORDINATION);

			} else {
				throw new RuntimeException(value + " not recougnised");
			}

		} else if (line.hasOption("stop")) {

			String value = line.getOptionValue("stop");

			if (value.equalsIgnoreCase("coordination")) {
				processorName = "stopCoordination";
				profiles.add(PROFILE.REST_CLIENT);
				profiles.add(PROFILE.CLI);
			} else {
				throw new RuntimeException(value + " not recougnised");
			}
		} else {


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

			if (line.hasOption("ls")) {
				processorName = "lsCommand";
			} else if (line.hasOption("status")) {
				processorName = "statusCommand";
			} else if (line.hasOption("count")) {
				processorName = "countCommand";
			} else if (line.hasOption("coordinationStatus")) {
				processorName = "coordinationStatusCommand";
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

		Option ls = OptionBuilder.hasOptionalArg().create("ls");

		Option start = OptionBuilder
				.hasOptionalArg()
				.withArgName("command")
				.withDescription(
						"e.g. coordination will start the coordination service")
				.create("start");

		Option stop = OptionBuilder.hasOptionalArg().withArgName("command")
				.withDescription("e.g. coordination will stop the coordination")
				.create("stop");

		Option agent = OptionBuilder.hasOptionalArg().withArgName("agentName")
		.withDescription("when provided with the ls command a list of agent names is provided, with the count command the count of the number of agents is provided")
		.create("agent");

		Option logType = OptionBuilder
		.withDescription("when provided with the ls command a list of agent names is provided, with the count command the count of the number of agents is provided")
		.create("logType");

		
		Option count = OptionBuilder.hasOptionalArg().create("count");

		Option from = OptionBuilder.hasArg().withArgName("from")
				.withDescription("from position (integer)").create("from");

		Option maxResults = OptionBuilder.hasArg().withArgName("max")
				.withDescription("max results position (integer)")
				.create("max");
		
		Option offline = OptionBuilder
				.withDescription(
						"Use only when the coordination is not running use with ls, status, update to connect to the database directory and not via rest")
				.create("o");

		Option json = OptionBuilder.withDescription("prints to json format ")
				.create("json");

		Option coordinationStatus = OptionBuilder
				.create("coordinationStatus");

		Option query = OptionBuilder
				.hasArg()
				.withArgName("query")
				.withDescription(
						"jpa query string e.g. lastModificationTime < 1000")
				.create("query");

		Options options = new Options();
		options.addOption(help);
		options.addOption(start);
		options.addOption(stop);
		options.addOption(ls);
		options.addOption(agent);
		options.addOption(logType);
		options.addOption(count);
		options.addOption(from);
		options.addOption(maxResults);
		options.addOption(json);
		options.addOption(query);
		options.addOption(offline);
		options.addOption(coordinationStatus);

		return options;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.specificmedia.hadoop.streams.agent.cli.impl.CommnadLineParser#getFactory
	 * ()
	 */
	@Override
	public CommandLineProcessorFactory getFactory() {
		return factory;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.specificmedia.hadoop.streams.agent.cli.impl.CommnadLineParser#setFactory
	 * (
	 * com.specificmedia.hadoop.streams.commons.cli.CommandLineProcessorFactory)
	 */
	@Override
	public void setFactory(CommandLineProcessorFactory factory) {
		this.factory = factory;
	}

}
