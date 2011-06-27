package org.streams.commons.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.streams.commons.file.FileStatus.FileTrackingStatus.Builder;

import com.googlecode.protobuf.format.JsonFormat;

/**
 * Encapsulates the logic of writing a FileTracingStatus instance to JSON or
 * Plain Text
 * 
 */
public class FileTrackingStatusFormatter {

	public static enum FORMAT {
		TXT, JSON
	}

	private static final ObjectMapper mapper = new ObjectMapper();

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
	public Collection<FileStatus.FileTrackingStatus> readList(FORMAT format,
			Reader reader) throws JsonParseException, JsonMappingException,
			IOException {

		Collection<FileStatus.FileTrackingStatus> coll = null;

		if (format.equals(FORMAT.JSON)) {
			Collection<String> strcoll = (Collection<String>) mapper.readValue(
					reader, new TypeReference<Collection<String>>() {
					});
			coll = new ArrayList<FileStatus.FileTrackingStatus>();


			
			for (String str : strcoll) {
				Builder builder = FileStatus.FileTrackingStatus.newBuilder();
				JsonFormat.merge(str, builder);
				coll.add(builder.build());
			}

		} else {
			BufferedReader buff = new BufferedReader(reader);
			coll = new ArrayList<FileStatus.FileTrackingStatus>();

			String line = null;
			while ((line = buff.readLine()) != null) {
				coll.add(read(FORMAT.TXT, line));
			}

		}

		return coll;
	}

	public Collection<FileStatus.FileTrackingStatus> writeList(FORMAT format,
			Collection<FileStatus.FileTrackingStatus> coll, Writer writer)
			throws JsonParseException, JsonMappingException, IOException {

		if (format.equals(FORMAT.JSON)) {
			Collection<String> strColl = new ArrayList<String>();
			for (FileStatus.FileTrackingStatus file : coll) {
				strColl.add(JsonFormat.printToString(file));
			}

			mapper.writeValue(writer, strColl);
		} else {
			for (FileStatus.FileTrackingStatus file : coll) {
				String line = write(FORMAT.TXT, file);
				writer.write(line + "\n");
			}
		}

		return coll;
	}

	public FileStatus.FileTrackingStatus read(FORMAT format, String str) {
		FileStatus.FileTrackingStatus file = null;

		if (format.equals(FORMAT.JSON)) {
			try {
				file = mapper.readValue(str,
						FileStatus.FileTrackingStatus.class);
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
	public String write(FORMAT format, FileStatus.FileTrackingStatus file) {
		String ret = null;

		if (format.equals(FORMAT.JSON)) {
			try {
				ret = JsonFormat.printToString(file);
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
	private static final FileStatus.FileTrackingStatus readTXT(String str) {
		// --- This is ugly and done fast we need to revise this to provide a
		// more elegant implementation.
		// --- Again time constraints prevail :(

		Builder file = null;

		String[] split = str.split("\t");
		if (split != null && split.length > 0) {
			file = FileStatus.FileTrackingStatus.newBuilder();

			file.setFileSize(Long.valueOf(split[0]));
			file.setLogType(split[1]);
			file.setFilePointer(Long.valueOf(split[2]));
			file.setLinePointer(Integer.valueOf(split[3]));
			file.setAgentName(split[4]);
			file.setFileName(split[5]);
		}
		return file.build();
	}

	/**
	 * Writes a tab separated string
	 * 
	 * @param file
	 * @return
	 */
	private static final String toTXT(FileStatus.FileTrackingStatus file) {
		// --- This is ugly and done fast we need to revise this to provide a
		// more elegant implementation.
		// --- Again time constraints prevail :(

		return file.getFileSize() + "\t" + file.getLogType() + "\t"
				+ file.getFilePointer() + "\t" + file.getLinePointer() + "\t"
				+ file.getAgentName() + "\t" + file.getFileName();
	}
}
