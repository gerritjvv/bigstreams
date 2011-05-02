package org.streams.agent.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * Encapsulates the logic of writing a FileTracingStatus instance to JSON or
 * Plain Text
 * 
 */
public class FileTrackingStatusFormatter {

	public static enum FORMAT {
		TXT, JSON
	}

	private static Logger LOG = Logger.getLogger(FileTrackingStatusFormatter.class);
	
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final DateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
	
	
	/**
	 * Reads a collection of FileTrackingStatus.<br/>
	 * If plain text is a list of line separated plain text.<br/>
	 * If json this should be a json array.<br/>
	 * 
	 * @param format
	 * @param reader
	 * @return
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public Collection<FileTrackingStatus> readList(FORMAT format, Reader reader)
			throws JsonParseException, JsonMappingException, IOException {

		Collection<FileTrackingStatus> coll = null;

		if (format.equals(FORMAT.JSON)) {
			coll = (Collection<FileTrackingStatus>) mapper.readValue(reader,
					new TypeReference<Collection<FileTrackingStatus>>() { });
		} else {
			BufferedReader buff = new BufferedReader(reader);
			coll = new ArrayList<FileTrackingStatus>();

			String line = null;
			while ((line = buff.readLine()) != null) {
				coll.add(read(FORMAT.TXT, line));
			}

		}

		return coll;
	}

	public Collection<FileTrackingStatus> writeList(FORMAT format, Collection<FileTrackingStatus> coll, Writer writer)
			throws JsonParseException, JsonMappingException, IOException {


		if (format.equals(FORMAT.JSON)) {
			mapper.writeValue(writer, coll);
		} else {
			for(FileTrackingStatus file: coll){
				String line = write(FORMAT.TXT, file);
				writer.write(line + "\n");
			}
		}

		return coll;
	}
	
	
	public FileTrackingStatus read(FORMAT format, String str) {
		FileTrackingStatus file = null;

		if (format.equals(FORMAT.JSON)) {
			try {
				file = mapper.readValue(str, FileTrackingStatus.class);
			} catch (Exception excp) {
				RuntimeException rte = new RuntimeException(excp.toString(),
						excp);
				rte.setStackTrace(excp.getStackTrace());
				throw rte;
			}
		} else {
			file = readTXT(str);
		}

		return file;
	}

	/**
	 * Writes as a string using the format specified the FileTrackingStatus
	 * 
	 * @param format
	 * @param file
	 * @return
	 */
	public String write(FORMAT format, FileTrackingStatus file) {
		String ret = null;

		if (format.equals(FORMAT.JSON)) {
			try {
				ret = mapper.writeValueAsString(file);
			} catch (Exception excp) {
				RuntimeException rte = new RuntimeException(excp.toString(),
						excp);
				rte.setStackTrace(excp.getStackTrace());
				throw rte;
			}
		} else {
			ret = toTXT(file);
		}

		return ret;
	}

	/**
	 * Returns null if no data is found after str.split("\t").
	 * 
	 * @param str
	 * @return
	 */
	private static final FileTrackingStatus readTXT(String str) {
		// --- This is ugly and done fast we need to revise this to provide a
		// more elegant implementation.
		// --- Again time constraints prevail :(

		FileTrackingStatus file = null;
		
		String[] split = str.split("\t");
		if (split != null && split.length > 0) {
			file = new FileTrackingStatus();

			file.setFileSize(Long.valueOf(split[0]));
			file.setLogType(split[1]);
			file.setStatus(FileTrackingStatus.STATUS.valueOf(split[2]));
			file.setPath(split[3]);
			file.setLastModificationTime(Long.valueOf(split[4]));
			file.setFilePointer(Long.valueOf(split[5]));
			file.setLinePointer(Integer.parseInt(split[6]));
			try {
				file.setFileDate(simpleDateFormat.parse(split[7]));
			} catch (ParseException e) {
				LOG.error(e.toString(), e);
			}
			try {
				file.setSentDate(simpleDateFormat.parse(split[8]));
			} catch (ParseException e) {
				LOG.error(e.toString(), e);
			}
		}
		return file;
	}

	/**
	 * Writes a tab separated string
	 * 
	 * @param file
	 * @return
	 */
	private static final String toTXT(FileTrackingStatus file) {
		// --- This is ugly and done fast we need to revise this to provide a
		// more elegant implementation.
		// --- Again time constraints prevail :(

		Date fileDate = file.getFileDate();
		Date sentDate = file.getSentDate();
		
		if(fileDate == null){
			fileDate = new Date();
		}
		if(sentDate == null){
			sentDate = new Date();
		}
		
		return file.getFileSize() + "\t" + file.getLogType() + "\t"
				+ file.getStatus() + "\t" + file.getPath() + "\t"
				+ file.getLastModificationTime() + "\t" + file.getFilePointer()
				+ "\t" + file.getLinePointer() 
				+ "\t" + simpleDateFormat.format(fileDate)
				+ "\t" + simpleDateFormat.format(sentDate);
		
	}

}
