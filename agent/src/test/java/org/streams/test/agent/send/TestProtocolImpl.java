package org.streams.test.agent.send;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Test;
import org.streams.agent.mon.status.impl.AgentStatusImpl;
import org.streams.commons.compression.CompressionPoolFactory;
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl;
import org.streams.commons.io.impl.ProtocolImpl;

/**
 * Test Protocol
 * 
 */
public class TestProtocolImpl extends TestCase {

	@Test
	public void testReadProtocolWrongHeaderLen() throws Exception {

		ByteArrayOutputStream output = new ByteArrayOutputStream();
		DataOutputStream datOut = new DataOutputStream(output);

		// write the wrong length for the header length.
		// no further data is needed as we expect the Protocol to fail on this
		try {
			byte[] namebytes = GzipCodec.class.getName().getBytes();
			datOut.writeInt(namebytes.length);
			datOut.write(namebytes);
			datOut.writeInt(1000);
			datOut.write("test".getBytes()); // this data is normally compressed
												// but we expect the code to
												// fail when reading the
			// lengths
		} finally {
			datOut.close();
			output.close();
		}

		CompressionPoolFactory pf = new CompressionPoolFactoryImpl(10, 10,
				new AgentStatusImpl());
		ProtocolImpl p = new ProtocolImpl(pf);

		Configuration conf = new SystemConfiguration();

		ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());
		try {
			p.read(conf, new DataInputStream(in));
			assertTrue(false);
		} catch (Throwable t) {
			assertTrue(true);
		}
	}

	@Test
	public void testReadProtocolWrongCompressionLen() throws Exception {

		ByteArrayOutputStream output = new ByteArrayOutputStream();
		DataOutputStream datOut = new DataOutputStream(output);

		// write the wrong length for the header.
		// no further data is needed as we expect the Protocol to fail on this
		try {
			datOut.writeInt(400);
			datOut.write(GzipCodec.class.getName().getBytes());
		} finally {
			datOut.close();
			output.close();
		}

		CompressionPoolFactory pf = new CompressionPoolFactoryImpl(10, 10,
				new AgentStatusImpl());

		ProtocolImpl p = new ProtocolImpl(pf);

		Configuration conf = new SystemConfiguration();

		ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());
		try {
			p.read(conf, new DataInputStream(in));
			assertTrue(false);
		} catch (Throwable t) {
			assertTrue(true);
		}
	}
}
