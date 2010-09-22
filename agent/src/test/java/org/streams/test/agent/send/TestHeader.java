package org.streams.test.agent.send;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.commons.io.Header;


/**
 * 
 * Tests the header POJO
 */
public class TestHeader extends TestCase{

	@Test
	public void testHashCodeNotEquals(){
		Header h1 = new Header();
			h1.setFileName("test");
		Header h2 = new Header();
		
		assertFalse( h1.hashCode() == h2.hashCode() );
	}
	
	@Test
	public void testHashCodeEquals(){
		
		Header h1 = new Header();
		Header h2 = new Header();
		assertEquals(h1.hashCode(), h2.hashCode());
		
		h1 = new Header();
		h1.setFileName("test");
		h2 = new Header();
		h2.setFileName("test");
		
		assertEquals(h1.hashCode(), h2.hashCode());
		
		
	}
	
	@Test
	public void testToJson(){
		
		Header header = new Header();
		String jsonString = header.toJsonString();
		assertNotNull(jsonString);
		
	}
	
	@Test
	public void testEquals(){
		
		Header h1 = new Header();
		Header h2 = new Header();
		
		assertTrue(h1.equals(h2));
		
	}
	@Test 
	public void testEquals1(){
		Header h1 = new Header();
		Header h2 = new Header();
		h2.setFileName("test2");
		
		assertFalse(h1.equals(h2));
	}
	
}
