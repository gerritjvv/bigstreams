package org.streams.test.agent.mon.impl;

import java.util.Date;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.TestCase;

import org.junit.Test;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.file.FileTrackingStatus.STATUS;
import org.streams.agent.file.impl.db.DBFileTrackerMemoryImpl;
import org.streams.agent.mon.status.impl.LateFileCalculator;

public class TestLateFileCalculator extends TestCase{

	EntityManagerFactory fact;
	DBFileTrackerMemoryImpl memory = null;	
	
	@Test
	public void testLateFiles(){
		
		LateFileCalculator calc = new LateFileCalculator(1, memory);
		assertEquals(9, calc.calulateLateFiles());
	}
	
	
	@Override
	protected void setUp() throws Exception {
		fact = Persistence.createEntityManagerFactory("fileTracking");
		memory = new DBFileTrackerMemoryImpl();
		memory.setEntityManagerFactory(fact);

		//create 9 normal files 2 in READY, 3 in READING, and 4 in PARKED
		Date now = new Date();
		
		//set date to 1 hour ago
		Date lateDate = new Date(System.currentTimeMillis() - (60 * 60 * 1000));
		
	    createFile(2, now, STATUS.READY);
		createFile(3, now, STATUS.READING);
		createFile(4, now, STATUS.PARKED);
		
		//create 9 late files 2 in READY , 3 in READING, and 4 in PARKED
		createFile(2, lateDate, STATUS.READY);
		createFile(3, lateDate, STATUS.READING);
		createFile(4, lateDate, STATUS.PARKED);
		
	}
	
	@Override
	protected void tearDown() throws Exception {
		fact.close();
	}

	private void createFile(int count, Date date, STATUS status){
		System.out.println("Adding files: " + status + ": " + count);
		for(int i = 0; i < count; i++){
			
		  memory.updateFile(new FileTrackingStatus(System.currentTimeMillis(), 
				100, "" + System.currentTimeMillis() + "." + i, status, 0, 0, "typetest", date, null));
		}
	}
	
}

