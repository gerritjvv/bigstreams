package org.streams.collector.write;

/**
 *
 * This interface is created to allow the following design:<br/>
 * <pre>
 * writer.write(file data, new PostWriteAction(){ run(){ syncService.release(sync) } )
 *</pre>
 *The design allows the writer to:<br/>
 *<ul>
 * <li>Writer the file data</li>
 * <li>Execute the PostWriteAction</li>
 * <li>On Any Error: roll back file</li>
 *</ul>
 */
public interface PostWriteAction {

	/**
	 * Called after the files data has been written.
	 * @param bytesWritten The number of bytes written to the file.
	 * @throws Exception
	 */
	void run(int bytesWritten) throws Exception;
	
}
