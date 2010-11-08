package org.streams.commons.io;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * Wraps multiple InputStreams into one. Streams are read from in the order they were added.
 * <br/>
 * This class is not thread safe, just as with most java.io classes.
 *
 */
public class CompositeInputStream extends InputStream{

	InputStream inputs[];
	int streamIndex = 0;
	
	InputStream currentInput;
	
	public CompositeInputStream(InputStream... inputs){
		this.inputs = inputs;
		currentInput = inputs[0];
	}
	
	
	@Override
	public void close() throws IOException{
		for( InputStream input : inputs){
			input.close();
		}
	}
	
	@Override
	public int read() throws IOException {
		
		int r = currentInput.read();
		if(r == -1){
			//end of current stream
			if(streamIndex < inputs.length){
				currentInput = inputs[streamIndex++];
				r = currentInput.read();
			}
		}
	
		return r;
	}
	
}
