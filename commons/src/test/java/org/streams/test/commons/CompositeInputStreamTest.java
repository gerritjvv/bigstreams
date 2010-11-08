package org.streams.test.commons;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.streams.commons.io.CompositeInputStream;

/**
 * 
 *  Tests the behaviour of the CompositeInputStream
 *
 */
public class CompositeInputStreamTest extends TestCase{

	private static final String TEST_STRING = "This is a text test string";
	
	InputStream inputs[];
	int inputStreamCount = 21;               
	
	
	/**
	 * 
	 * Tests that the test lines can be read correctly
	 * @throws IOException 
	 * 
	 */
	@Test
	public void testReadAllStreams() throws IOException{
		
		CompositeInputStream cin = new CompositeInputStream(inputs);
		BufferedReader reader = new BufferedReader(new InputStreamReader(cin));
		
		String line = null;
		
		int count = 0;
		try{
			while( (line = reader.readLine()) != null){
				assertEquals( TEST_STRING, line);
				count++;
			}
		}finally{
			reader.close();
		}
		
		assertEquals(inputStreamCount, count);
		
	}
	
	
	@Before
	public void setUp() throws Exception {
		
		byte[] testStringBytes = (TEST_STRING + "\n").getBytes("UTF-8");
		
		inputs = new InputStream[inputStreamCount];
		
		for(int i = 0; i < inputStreamCount; i++){
			
			InputStream in = new ByteArrayInputStream(testStringBytes);
			
			inputs[i] = in;
		}
		
	}

	@After
	public void tearDown() throws Exception {
	}

	
	
	
}
