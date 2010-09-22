package org.streams.test.agent.startup.check.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import junit.framework.TestCase;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.junit.Test;
import org.streams.agent.agentcli.startup.check.impl.CodecCheck;
import org.streams.agent.main.Bootstrap;
import org.streams.commons.cli.CommandLineProcessorFactory;


/**
 * 
 * Runs the CodecCheck
 */
public class TestCodecCheck extends TestCase {

	Bootstrap bootstrap;

	/**
	 * Sets a compression codec that returns null on all methods
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCodecCheckFailWithErrorCodec() throws Exception {
		CodecCheck check = new CodecCheck();
		check.setCodec(new CompressionCodec() {

			@Override
			public String getDefaultExtension() {
				return null;
			}

			@Override
			public Class<? extends Decompressor> getDecompressorType() {
				return null;
			}

			@Override
			public Class<? extends Compressor> getCompressorType() {
				return null;
			}

			@Override
			public CompressionOutputStream createOutputStream(
					OutputStream arg0, Compressor arg1) throws IOException {
				return null;
			}

			@Override
			public CompressionOutputStream createOutputStream(OutputStream arg0)
					throws IOException {
				return null;
			}

			@Override
			public CompressionInputStream createInputStream(InputStream arg0,
					Decompressor arg1) throws IOException {
				return null;
			}

			@Override
			public CompressionInputStream createInputStream(InputStream arg0)
					throws IOException {
				return null;
			}

			@Override
			public Decompressor createDecompressor() {
				return null;
			}

			@Override
			public Compressor createCompressor() {
				return null;
			}
		});

		try {
			check.runCheck();
			assertTrue(false);
		} catch (Throwable t) {
			assertTrue(true);
		}
	}

	/**
	 * Sets null for the codec
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCodecCheckFailWithNullCodec() throws Exception {

		CodecCheck check = new CodecCheck();
		check.setCodec(null);
		try {
			check.runCheck();
			assertTrue(false);
		} catch (Throwable t) {
			assertTrue(true);
		}
	}

	/**
	 * Uses the codecCheck returned from the DI
	 */
	@Test
	public void testCodecCheck() {

		CodecCheck check = (CodecCheck) bootstrap.getBean("codecCheck");

		try {
			check.runCheck();
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(false);
		}

	}

	@Override
	protected void setUp() throws Exception {
		bootstrap = new Bootstrap();
		bootstrap.loadProfiles(CommandLineProcessorFactory.PROFILE.DB,
				CommandLineProcessorFactory.PROFILE.AGENT);
	}

}
