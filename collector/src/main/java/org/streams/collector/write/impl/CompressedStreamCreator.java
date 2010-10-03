package org.streams.collector.write.impl;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.streams.collector.write.StreamCreator;
import org.streams.commons.compression.CompressionPool;

/**
 * 
 * Implements the needed logic to copy between to compressed files.<br/>
 * Note that the compressed files must have the same compression format.
 * 
 */
public class CompressedStreamCreator implements StreamCreator {

	CompressionCodec codec;
	CompressionPool pool;
	OutputStream out;

	long waitForCompressionResource;

	public CompressedStreamCreator(CompressionCodec codec,
			CompressionPool pool, long waitForCompressionResource) {
		super();
		this.codec = codec;
		this.pool = pool;
		this.waitForCompressionResource = waitForCompressionResource;
	}

	public void transfer(File from, File to, long mark) throws IOException,
			InterruptedException {

		out = CompressionRollBackHelper.copy(from, to,
				waitForCompressionResource, codec, pool, mark);
	}

	public void markEvent(File file, OutputStream out) {

	}

	public void close(File file, OutputStream out){
		
		//this output stream MUST be a CompressionOutputStream
		CompressionOutputStream cout = ((CompressionOutputStream)out);
		try {
			cout.finish();
		} catch (IOException e) {
			//FINISH QUITELY
			e.printStackTrace();
		}
		pool.closeAndRelease(cout);
		
	}
	
	@Override
	public OutputStream create(File file) throws IOException {
		OutputStream fout = out;
		out = null;
		return fout;
	}

}
