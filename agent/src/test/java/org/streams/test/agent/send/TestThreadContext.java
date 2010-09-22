package org.streams.test.agent.send;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.send.ThreadContext;


/**
 * The ThreadContext is a simple pojo.<br/>
 * This tests are purely for Test coverage.<br/> 
 *
 */
public class TestThreadContext extends TestCase{
	
	@Test
	public void testSetterMethods(){
		
		ThreadContext ctx = new ThreadContext(null, null, null, null, null, 1000, 1);
		
		ctx.setClient(null);
		ctx.setCollectorAddress(null);
		ctx.setMemory(null);
		ctx.setQueue(null);
		ctx.setRetries(10);
		ctx.setWaitIfEmpty(1000);
		
	}

}
