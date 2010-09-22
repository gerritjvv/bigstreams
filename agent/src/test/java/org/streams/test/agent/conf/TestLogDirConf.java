package org.streams.test.agent.conf;

import java.io.File;
import java.io.FileWriter;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.streams.agent.conf.LogDirConf;


/**
 * Tests that the LogDirConf reads conf files as expected.
 *
 */
public class TestLogDirConf extends TestCase{

	File baseDir;
	File[] testDirs;
	
	File testConfFile;
	File testWrongFormatConfFile;
	File testDirNotExistConfFile;
	File testDuplicateDirConfFile;
	
	@Test
	public void testDuplicateDirConfFile(){
		
		try{
			new LogDirConf(testDuplicateDirConfFile.getAbsolutePath());
			assertTrue(false);
		}catch(Throwable t){
			assertTrue(true);
		}
	}
	
	
	@Test
	public void testDirNotExistConfFile(){
		
		try{
			new LogDirConf(testDirNotExistConfFile.getAbsolutePath());
			assertTrue(false);
		}catch(Throwable t){
			assertTrue(true);
		}
	}
	
	
	@Test
	public void testWrongFormatConfFile(){
		
		try{
			new LogDirConf(testWrongFormatConfFile.getAbsolutePath());
			assertTrue(false);
		}catch(Throwable t){
			assertTrue(true);
		}
	}
	
	@Test
	public void testCorrectConf()throws Exception{
		LogDirConf logDirConf = new LogDirConf(testConfFile.getAbsolutePath());
		
		
		for(File dir: logDirConf.getDirectories()){
			
			assertTrue(dir.isDirectory());
			assertNotNull(logDirConf.getLogType(dir));
			
		}
		
		
	}
	
	
	
	@Override
	protected void setUp() throws Exception {
		
		baseDir = new File(".", "target/testLogDirConf");
		baseDir.mkdirs();
		
		int len = 5;
		testDirs = new File[len];
		
		for(int i = 0; i < len; i++){
			testDirs[i] = new File(baseDir, "testDir_" + i);
			testDirs[i].mkdirs();
		}
	
		//---- Create correct conf file
		testConfFile = File.createTempFile("testconffile", ".txt");
		FileWriter writer = new FileWriter(testConfFile);
		try{
			for(int i = 0; i < len; i++){
				writer.append("\ntest" + i + " " + testDirs[i].getAbsolutePath());
			}
		}finally{
			writer.close();
		}
		
		//-- Create wrong format conf file
		testWrongFormatConfFile = File.createTempFile("testconffileWrongFormat", ".txt");
		writer = new FileWriter(testWrongFormatConfFile);
		try{
			for(int i = 0; i < len; i++){
				writer.append("\n" + testDirs[i].getAbsolutePath());
			}
		}finally{
			writer.close();
		}
		
		//-- Create wrong format conf file
		testDirNotExistConfFile = File.createTempFile("testDirNotExistConfFile", ".txt");
		writer = new FileWriter(testDirNotExistConfFile);
		try{
			for(int i = 0; i < len; i++){
				writer.append("\ntest" + i + " " + testDirs[i].getAbsolutePath() + ".notexist");
			}
		}finally{
			writer.close();
		}
		
		//-- Create duplicate dir conf file
		testDuplicateDirConfFile = File.createTempFile("testDuplicateDirConfFile", ".txt");
		writer = new FileWriter(testDuplicateDirConfFile);
		try{
			for(int i = 0; i < len; i++){
				writer.append("\ntest" + i + " " + testDirs[i].getAbsolutePath());
			}
			
			writer.append("\ntest" + 10 + " " + testDirs[0].getAbsolutePath());
		}finally{
			writer.close();
		}
		
	}

	@Override
	protected void tearDown() throws Exception {
		FileUtils.deleteDirectory(baseDir);

		testConfFile.delete();
		testWrongFormatConfFile.delete();
		testDirNotExistConfFile.delete();
		testDuplicateDirConfFile.delete();
	}

	
	
}
