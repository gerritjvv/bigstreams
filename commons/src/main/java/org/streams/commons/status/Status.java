package org.streams.commons.status;

public interface Status {

	enum STATUS {
		SERVER_ERROR, CLIENT_ERROR, UNKOWN_ERROR, OK, SHUTDOWN, COORDINATION_ERROR, COORDINATION_LOCK_ERROR,
		HEARTBEAT_ERROR
	};

	void setCounter(String status, int counter);

	STATUS getStatus();

	String getStatusMessage();

	void setStatus(STATUS status, String msg);

}
