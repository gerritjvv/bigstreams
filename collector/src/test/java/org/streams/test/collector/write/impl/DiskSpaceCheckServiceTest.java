package org.streams.test.collector.write.impl;

import static org.junit.Assert.*;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;
import org.streams.collector.conf.DiskSpaceFullActionConf;
import org.streams.collector.write.impl.DiskSpaceCheckService;

public class DiskSpaceCheckServiceTest {

	
	@Test
	public void testAction(){
		
		Configuration configuration = new PropertiesConfiguration();
		configuration.setProperty("diskfull.action", "DIE");
		

		DiskSpaceFullActionConf conf = new DiskSpaceFullActionConf(configuration);
		assertEquals(DiskSpaceFullActionConf.ACTION.DIE, conf.getDiskAction());
	}
	
	@Test
	public void testDefaultAction(){
		
		Configuration configuration = new PropertiesConfiguration();
		

		DiskSpaceFullActionConf conf = new DiskSpaceFullActionConf(configuration);
		assertEquals(DiskSpaceFullActionConf.ACTION.ALERT, conf.getDiskAction());
	}
	
}
