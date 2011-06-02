package org.streams.collector.server.impl;

/**
 * 
 * A simple pojo that contains the write request session details from the agent.
 * 
 */
public class AgentSession {

	volatile String remoteAddress;
	volatile String agentName;
	volatile long requestStartTime = 0L;
	volatile long fileWriteStartTime = 0L;
	volatile long fileWriteEndTime = 0L;

	volatile String fileName = null;
	volatile String logType = null;

	volatile boolean messageReceived = false;
	volatile boolean acquiredCoordinationLock = false;
	volatile boolean releasedCoordinationLock = false;

	volatile boolean writtenToFile = false;
	volatile boolean sentResponseRequest = false;

	public AgentSession(String remoteAddress) {
		this(remoteAddress, System.currentTimeMillis());
	}

	public AgentSession(String remoteAddress, long requestStartTime) {
		super();
		this.remoteAddress = remoteAddress;
		this.requestStartTime = requestStartTime;
	}

	public String getAgentName() {
		return agentName;
	}

	public long getRequestStartTime() {
		return requestStartTime;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public boolean isAcquiredCoordinationLock() {
		return acquiredCoordinationLock;
	}

	public void setAcquiredCoordinationLock() {
		this.acquiredCoordinationLock = true;
	}

	public boolean isWrittenToFile() {
		return writtenToFile;
	}

	public void setWrittenToFile() {
		this.writtenToFile = true;
		this.fileWriteEndTime = System.currentTimeMillis();
	}

	public boolean isSentResponseRequest() {
		return sentResponseRequest;
	}

	public void setSentResponseRequest() {
		this.sentResponseRequest = true;
	}

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	public void setMessageReceived(){
		messageReceived = true;
	}
	public boolean getMessageReceived(){
		return messageReceived;
	}
	
	public boolean isReleasedCoordinationLock() {
		return releasedCoordinationLock;
	}

	public void setReleasedCoordinationLock() {
		this.releasedCoordinationLock = true;
	}

	public long getFileWriteStartTime() {
		return fileWriteStartTime;
	}

	public void setFileWriteStartTime() {
		this.fileWriteStartTime = System.currentTimeMillis();
	}

	public long getFileWriteEndTime() {
		return fileWriteEndTime;
	}

	public String toString() {
		return  "Address: " + remoteAddress +   
				" agent: " + agentName + " timeElapsed: "
				+ (System.currentTimeMillis() - requestStartTime)
				+ " messageReceived: " + messageReceived
				+ " fileName: " + fileName + " logType: " + logType
				+ " acquiredCoordinationLock: " + acquiredCoordinationLock
				+ " writtenToFile: " + writtenToFile + " sentResponseRequest: "
				+ sentResponseRequest + " releasedCoordinationLock: "
				+ releasedCoordinationLock + " fileWriteTimeTaken: "
				+ (fileWriteEndTime - fileWriteStartTime);
	}

	public String getRemoteAddress() {
		return remoteAddress;
	}

	public void setRemoteAddress(String remoteAddress) {
		this.remoteAddress = remoteAddress;
	}

	public void setAgentName(String agentName) {
		this.agentName = agentName;
	}

}
