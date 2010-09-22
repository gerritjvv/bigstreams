package org.streams.agent.cli.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.io.PrintWriter;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.cli.CommandLine;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.streams.agent.file.FileTrackerMemory;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.FileTrackingStatusFormatter;
import org.streams.commons.cli.CommandLineProcessor;


/**
 * Implements the update command
 * 
 */
@Named("updateCommand")
public class UpdateCommand implements CommandLineProcessor {

	private static final Logger LOG = Logger.getLogger(UpdateCommand.class);
	
	FileTrackingStatusFormatter formatter = new FileTrackingStatusFormatter();
	
	FileTrackerMemory memory;

	public UpdateCommand(){}
	public UpdateCommand(FileTrackerMemory memory) {
		this.memory = memory;
	}

	/**
	 * Expects a key[=:]value[,;]key[=:]value string only the keys specified and recougnised would be updated.
	 */
	@Override
	public void process(CommandLine cmdLine, OutputStream out) throws Exception {

		
		String path = cmdLine.getOptionValue("update");

		if (path == null || path.trim().length() < 1) {
			throw new RuntimeException(
					"Please provide a path argument for status");
		}
		
		String updateValues = cmdLine.getOptionValue("values");

		if (updateValues == null || updateValues.trim().length() < 1) {
			throw new RuntimeException(
					"Please type in the values argument");
		}


		PrintWriter writer = new PrintWriter(out);
		try {

			FileTrackingStatus file = memory.getFileStatus(new File(path));

			if (file == null) {
				throw new FileNotFoundException(path);
			}
			LOG.info("Updaging " + path + " with values " + updateValues);
			
			file.fill(updateValues);
			memory.updateFile(file);
			
			LOG.info("Memory: " + memory);
			
			//get the file status again and output to console
			file = memory.getFileStatus(new File(path));
			
			String updatedData = null;
			if(cmdLine.hasOption("json")){
				updatedData = formatter.write(FileTrackingStatusFormatter.FORMAT.JSON, file);
			}else{
				updatedData = formatter.write(FileTrackingStatusFormatter.FORMAT.TXT, file);
			}
			
			writer.println(updatedData);
			
			LOG.info("Completed update of file: " + file);
			
		} finally {
			writer.close();
		}

	}
	
	@Autowired(required=false)
	public void setMemory(FileTrackerMemory memory) {
		this.memory = memory;
	}

}
