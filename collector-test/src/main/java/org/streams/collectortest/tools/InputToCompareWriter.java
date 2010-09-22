package org.streams.collectortest.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.streams.collector.conf.CollectorProperties;
import org.streams.collectortest.tools.util.ConfigurationLoader;
import org.streams.commons.util.CompressionCodecLoader;


/**
 * 
 * This tool accepts 3 arguments:
 * <ul>
 * <li>The file to write all agents files into</li>
 * <li>The file to write all collector files into</li>
 * </ul>
 * <br/>
 * Compression:<br/>
 * This class will handle collector compression by loading the java.library.path
 * variable either from the environment variables or from the configuration
 * properties files.<br/>
 * If compression is enabled on the collector configuration this class will use
 * the CompressionCodec specified in the configuration to read the files.<br/>
 * <p/>
 * The result and purpose of this class is the combined the agent log files into
 * one and the collector output files into another file to enable comparing the
 * contents.
 * 
 */
public class InputToCompareWriter {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			throw new RuntimeException(
					"Please type <agent file to create> <collector file to create> <original log files dir>");
		}

		File agentOneFile = new File(args[0]);
		File collectorOneFile = new File(args[1]);
		File logFileDir = new File(args[2]);

		// we need to have the conf.properties and the
		// streams-collector.properties in the classpath
		Configuration conf = ConfigurationLoader.loadConf();

		File collectorDir = new File(conf.getString(
				CollectorProperties.WRITER.BASE_DIR.toString(),
				(String) CollectorProperties.WRITER.BASE_DIR.getDefaultValue()));

		// write out the original log files to the comparable file
		writeLogFilesToOne(null, logFileDir, agentOneFile);

		boolean compressionEnabled = conf.getBoolean(
				CollectorProperties.WRITER.LOG_COMPRESS_OUTPUT.toString(),
				(Boolean) CollectorProperties.WRITER.LOG_COMPRESS_OUTPUT
						.getDefaultValue());

		String compressionCodec = conf.getString(
				CollectorProperties.WRITER.LOG_COMPRESSION_CODEC.toString(),
				(String) CollectorProperties.WRITER.LOG_COMPRESSION_CODEC
						.getDefaultValue());

		writeLogFilesToOne(
				(compressionEnabled) ? CompressionCodecLoader.loadCodec(conf,
						compressionCodec) : null, collectorDir,
				collectorOneFile);

	}

	/**
	 * Get all of the files from the agent one
	 * 
	 * @param logFileDir
	 * @param resultFile
	 */
	private static void writeLogFilesToOne(CompressionCodec codec,
			File logFileDir, File resultFile) throws IOException {

		BufferedWriter writer = new BufferedWriter(new FileWriter(resultFile));
		try {
			for (File file : logFileDir.listFiles()) {

				Reader reader = createReader(codec, file);
				try {
					IOUtils.copy(reader, writer);
				} finally {
					reader.close();
				}

			}
		} finally {
			writer.close();
		}

	}

	/**
	 * Creates a un compress reader if the codec is not null, else a normal file
	 * reader is created.
	 * 
	 * @param codec
	 * @param file
	 * @return
	 */
	private static final Reader createReader(CompressionCodec codec, File file)
			throws IOException {

		if (codec == null) {
			return new BufferedReader(new FileReader(file));
		} else {
			return new BufferedReader(new InputStreamReader(
					codec.createInputStream(new FileInputStream(file))));
		}

	}

}
