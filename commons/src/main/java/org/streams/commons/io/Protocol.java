package org.streams.commons.io;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;

public interface Protocol {
	public Header read(Configuration conf, DataInputStream inputStream)
			throws IOException;

	public void send(Header header, CompressionCodec codec, DataOutput dataOut)
	throws IOException;
}
