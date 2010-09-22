package org.streams.agent.agentcli.startup.check.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.log4j.Logger;
import org.streams.commons.app.AbstractStartupCheck;


@Named
public class CodecCheck extends AbstractStartupCheck {
	private static final Logger LOG = Logger.getLogger(CodecCheck.class);

	CompressionCodec codec;

	public CodecCheck(){}
	
	public CodecCheck(CompressionCodec codec) {
		super();
		this.codec = codec;
	}

	@Override
	public void runCheck() throws Exception {

		LOG.info("Checking CODEC ");

		checkTrue(codec != null, "No Codec prodivded");

		// test codec by writing a stream and reading it
		File file = File.createTempFile("testCodec",
				"." + codec.getDefaultExtension());

		String testString = "This is a test string to test if the codec actually works by writing and reading the same string";
		byte[] testBytes = testString.getBytes();

		// Compress String
		FileOutputStream fileOut = new FileOutputStream(file);
		CompressionOutputStream out = codec.createOutputStream(fileOut);
		try {
			out.write(testString.getBytes());
			out.finish();
		} finally {
			IOUtils.closeQuietly(out);
			IOUtils.closeQuietly(fileOut);
		}

		// Un-Compress String
		String returnString = null;

		FileInputStream fileIn = new FileInputStream(file);
		CompressionInputStream in = codec.createInputStream(fileIn);
		try {
			byte[] readInBytes = new byte[testBytes.length];
			int bytesRead = in.read(readInBytes);
			returnString = new String(readInBytes, 0, bytesRead);
		}catch(IOException t){
			checkTrue(false, "Failed to compress and decompress a simple string with the codec "
					+ codec + " provided");
		}finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(fileIn);
		}

		checkTrue(testString.equals(returnString),
				"Failed to compress and decompress a simple string with the codec "
						+ codec + " provided");

		file.deleteOnExit();

		LOG.info("DONE");
	}

	public CompressionCodec getCodec() {
		return codec;
	}

	@Inject
	public void setCodec(CompressionCodec codec) {
		this.codec = codec;
	}

}
