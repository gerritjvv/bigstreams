package org.streams.collector.write.impl;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.streams.collector.write.LogRollover;


/**
 * This class will do a log roll over.<br/>
 * The rollover technique used differs from that of log4j only in that the extenion of a file is always preserved.<br/>
 * That is if the file ends in .gz, .txt, .lzo, .bz2 etc the rollover index will happen before the extension and the file<br/>
 * name will be something like 1.gz, 1.txt, 1.lzo, 1.bz2 etc.
 * </p>
 * This class is not thread safe.
 * 
 */
public class SimpleLogRollover implements LogRollover{

	private static final Logger LOG = Logger.getLogger(SimpleLogRollover.class);
	
	/**
	 * 
	 * @param file File the file to rollover
	 * @return File the rolled over file
	 */
	public File rollover(File file) throws IOException{
		
		//the file must exist and its parent directory must be writable
		if( !file.exists()  ){
			throw new IOException("The file does not exist " + file.getAbsolutePath());
		}
		
		if( !file.getParentFile().canWrite() ){
			throw new IOException("The directory " + file.getParent() + " must be writable");
		}
		
		String fileName = file.getAbsolutePath();
		
		//for now start the process at 0
		String rollFilename = createRollfileName(fileName, System.nanoTime());
		   
		File fileNew = new File(rollFilename); 
		   
		FileUtils.moveFile(file, fileNew);
		
		if(file.exists())
			FileUtils.forceDelete(file);
		
		LOG.info("Rolled file: " + file.getAbsolutePath() + " to " + fileNew.getAbsolutePath());
		
		
		return fileNew;
	}
	
	private String createRollfileName(String fileName, long rolloverIndex){
		String extension = FilenameUtils.getExtension(fileName);
		
		String ret = null;
		
		//if the fileName has a known compression extension, roll with last suffix being the compression extension
		if(extension == null || extension.length() < 1){
			ret = fileName + '.'  + rolloverIndex;
		}else{
			ret = FilenameUtils.removeExtension(fileName) + '.'+ rolloverIndex + '.' + extension;
		}
		

		
		return ret;
	}
	
}