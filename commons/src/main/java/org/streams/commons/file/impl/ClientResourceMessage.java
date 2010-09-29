package org.streams.commons.file.impl;

public class ClientResourceMessage {

	int code;
	String msg;

	boolean hasError;
	Throwable error;

	public ClientResourceMessage(int code, String msg, boolean hasError,
			Throwable error) {
		super();
		this.code = code;
		this.msg = msg;
		this.hasError = hasError;
		this.error = error;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public boolean isHasError() {
		return hasError;
	}

	public void setHasError(boolean hasError) {
		this.hasError = hasError;
	}

	public Throwable getError() {
		return error;
	}

	public void setError(Throwable error) {
		this.error = error;
	}
	
}
