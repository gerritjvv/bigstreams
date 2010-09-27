package org.streams.agent.cli.impl;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.commons.cli.CommandLineProcessor;
import org.streams.commons.cli.CommandLineProcessorFactory;
import org.streams.commons.cli.CommandLineProcessorFactory.PROFILE;

/**
 * Provides a command line interface with basic commands to view and manipulate
 * the FileTrackingStatus.<br/>
 * Commands:<br/>
 * <ul>
 * <li>help</li>
 * <li>ls from to status</li>
 * <li>count status</li>
 * <li>status filename</li>
 * <li>update filename "fields"</li>
 * <li>start [agent]</li>
 * <li>stop [agent]</li>
 * <li>statusAgent</li>s
 * </ul>
 * 
 */
public class AgentCommandLineParser implements
		org.streams.commons.cli.CommandLineParser {

	private static final String STATUS_STR = Arrays
			.toString(FileTrackingStatus.STATUS.values());

	CommandLineProcessorFactory factory;

	public AgentCommandLineParser(CommandLineProcessorFactory factory) {
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

			if (value.equalsIgnoreCase("agent")) {

				processorName = "startAgent";
				profiles.add(PROFILE.DB);
				profiles.add(PROFILE.AGENT);

			} else {
				throw new RuntimeException(value + " not recougnised");
			}

		} else if (line.hasOption("stop")) {

			String value = line.getOptionValue("stop");

			if (value.equalsIgnoreCase("agent")) {
				processorName = "stopAgent";
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

			if (line.hasOption("ls")) {
				processorName = "lsCommand";
			} else if (line.hasOption("status")) {
				processorName = "statusCommand";
			} else if (line.hasOption("update")) {
				processorName = "updateCommand";
			} else if (line.hasOption("count")) {
				processorName = "countCommand";
			} else if (line.hasOption("agentStatus")) {
				processorName = "agentStatusCommand";
			} else {

				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("agent", buildOptions());
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
		Option status = OptionBuilder.hasArg().withArgName("file name")
				.create("status");

		Option ls = OptionBuilder.hasOptionalArg().withArgName("status")
				.withDescription(STATUS_STR).create("ls");

		Option start = OptionBuilder.hasOptionalArg().withArgName("command")
				.withDescription("e.g. agent will start the agent")
				.create("start");

		Option stop = OptionBuilder.hasOptionalArg().withArgName("command")
				.withDescription("e.g. agent will stop the agent")
				.create("stop");

		Option count = OptionBuilder.hasOptionalArg().withArgName("status")
				.withDescription(STATUS_STR).create("count");

		Option update = OptionBuilder.hasOptionalArg().withArgName("file name")
				.create("update");

		Option from = OptionBuilder.hasArg().withArgName("from")
				.withDescription("from position (integer)").create("from");

		Option maxResults = OptionBuilder.hasArg().withArgName("max")
				.withDescription("max results position (integer)")
				.create("max");

		Option offline = OptionBuilder
				.withDescription(
						"Use only when the agent is not running use with ls, status, update to connect to the database directory and not via rest")
				.create("o");

		Option updateValues = OptionBuilder
				.hasOptionalArg()
				.withArgName("values")
				.withDescription(
						"key[=:]value[,;]key=value ( fields supported are e.g. path=test2.txt,logType=logType2,fileSize=2,filePointer=3,lastModificationTime=4,linePointer=5,status=DONE)")
				.create("values");

		Option json = OptionBuilder.withDescription("prints to json format ")
				.create("json");

		Option agentStatus = OptionBuilder.withDescription(
				"Queries agent process").create("agentStatus");

		Option query = OptionBuilder
				.hasArg()
				.withArgName("query")
				.withDescription(
						"jpa query string e.g. lastModificationTime < 1000 or path='test1.txt'")
				.create("query");

		Options options = new Options();
		options.addOption(help);
		options.addOption(start);
		options.addOption(stop);
		options.addOption(ls);
		options.addOption(status);
		options.addOption(count);
		options.addOption(update);
		options.addOption(updateValues);
		options.addOption(from);
		options.addOption(maxResults);
		options.addOption(json);
		options.addOption(query);
		options.addOption(offline);
		options.addOption(agentStatus);

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
