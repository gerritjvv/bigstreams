package org.streams.agent.send.impl;

import java.io.BufferedReader;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.streams.agent.file.FileLinePointer;
import org.streams.agent.send.FileStreamer;
import org.streams.commons.io.Header;


/**
 * 
 * This is the ClientHandlerContext for a ClientHandler It keeps track of the
 * status of the message transfer.
 * <p/>
 * Detecting client errors:<br/>
 * The clientStatusCode will be set to 500 if any client error appears<br/>
 * The variable errorCause will be populated with any errors.<br/>
 * <p/>
 * Detecting server errors:<br/>
 * The serverStatusCode is by default 500 (i.e at error), it will only be set to
 * 200 (OK) if and only if this message i.e. 200 was received from the server.
 * <p/>
 * File Lines Sent:<br/>
 * The FileLinePointer state is 0 before sending and will be incremented by the
 * FileLineStreamer when data is sent.<br/>
 * 
 */
public class ClientHandlerContext {

	public final static int STATUS_ERROR = 500;
	public final static int NO_SERVER_RESPONSE = 506;
	public final static int STATUS_OK = 200;
	public final static int STATUS_CONFLICT = 409;
	
	/**
	 * The value is set by the SimpleChannelUpStreamHandler on receipt of a
	 * server response.<br/>
	 * The response must be 200 any other value is interpreted as an error.
	 */
	final AtomicInteger serverStatusCode = new AtomicInteger(NO_SERVER_RESPONSE);
	/**
	 * Any error on the client site will switch this value to error.
	 */
	final AtomicInteger clientStatusCode = new AtomicInteger(STATUS_OK);

	final private FileLinePointer intermediatePointer = new FileLinePointer();

	final private Header header;
	final private BufferedReader reader;
	final private FileStreamer fileLineStreamer;

	final AtomicBoolean logDataSent = new AtomicBoolean(false);

	Throwable errorCause = null;

	public ClientHandlerContext(Header header, BufferedReader reader,
			FileStreamer fileLineStreamer) {
		super();
		this.header = header;
		this.reader = reader;
		this.fileLineStreamer = fileLineStreamer;
	}

	public void setServerStatusCode(int code) {
		serverStatusCode.set(code);
	}

	public int getServerStatusCode() {
		return serverStatusCode.get();
	}

	public void setClientStatusCode(int code) {
		clientStatusCode.set(code);
	}

	public int getClientStatusCode() {
		return clientStatusCode.get();
	}

	public FileLinePointer getIntermediatePointer() {
		return intermediatePointer;
	}

	public Header getHeader() {
		return header;
	}

	public BufferedReader getReader() {
		return reader;
	}

	public FileStreamer getFileLineStreamer() {
		return fileLineStreamer;
	}

	public void setLogDataSent(boolean status) {
		logDataSent.set(status);
	}

	public boolean getLogDataSent() {
		return logDataSent.get();
	}

	public Throwable getErrorCause() {
		return errorCause;
	}

	public void setErrorCause(Throwable errorCause) {
		this.errorCause = errorCause;
	}

}
