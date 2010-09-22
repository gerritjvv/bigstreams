package org.streams.commons.cli;

import java.io.OutputStream;

import org.apache.commons.cli.CommandLine;

/**
 * The CommandLineParser will use the CommandLineProcessor interface to
 * abstract the command implementations injected into it.
 * 
 */
public interface CommandLineProcessor {

	public void process(CommandLine cmdLine, OutputStream out) throws Exception;

}
