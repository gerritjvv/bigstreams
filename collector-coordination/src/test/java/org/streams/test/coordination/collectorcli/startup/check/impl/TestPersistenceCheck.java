package org.streams.test.coordination.collectorcli.startup.check.impl;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.coordination.cli.startup.check.impl.PersistenceCheck;


/**
 * 
 * Simple test for PersistenceCheck.
 */
public class TestPersistenceCheck extends TestCase {

	private static final String TEST_ENTITY = "coordinationFileTracking";

	@Test
	public void testCheckFail() throws Exception {
		
		PersistenceCheck check = new PersistenceCheck();

		try {
			check.runCheck();
			assertTrue(false);
		} catch (Throwable t) {
			assertTrue(true);
		}
	}
	
	@Test
	public void testCheckOK() throws Exception {
		EntityManagerFactory emf = Persistence
				.createEntityManagerFactory(TEST_ENTITY);

		PersistenceCheck check = new PersistenceCheck();
		check.setEntityManagerFactory(emf);

		try {
			check.runCheck();
			assertTrue(true);
		} catch (Throwable t) {
			t.printStackTrace();
			assertTrue(false);
		}finally{
			emf.close();
		}
	}

}
