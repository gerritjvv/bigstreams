package org.streams.commons.cli;

import java.io.OutputStream;


public interface CommandLineParser {

	/**
	 * Parse the arguments and delegate the work to the correct
	 * CommandLineProcessor
	 * 
	 * @param out
	 * @param arguments
	 * @return CommandLineProcess This is for testing and returns the command
	 *         line processor that was chosen as per the options
	 */
	public abstract CommandLineProcessor parse(OutputStream out,
			String[] arguments) throws Exception;

	public abstract CommandLineProcessorFactory getFactory();

	public abstract void setFactory(CommandLineProcessorFactory factory);

}