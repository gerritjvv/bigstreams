package org.streams.agent.send;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.streams.agent.file.FileLinePointer;


public interface Client {

	public void open(InetSocketAddress address,
			FileLinePointer fileLinePointer, File file) throws IOException;

	public void close();

	public boolean sendCunk(long uniqueId, String logType) throws IOException;

	public FileStreamer getFileStreamer();

	public void setFileStreamer(FileStreamer fileStreamer);

	public ClientConnectionFactory getClientConnectionFactory();

	public void setClientConnectionFactory(
			ClientConnectionFactory clientConnectionFactory);
}
