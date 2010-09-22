package org.streams.collector.write;

public class WriterException extends RuntimeException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public WriterException(){}
	public WriterException(String msg){
		super(msg);
	}
	
	public WriterException(String msg, Throwable t){
		super(msg, t);
	}
}
