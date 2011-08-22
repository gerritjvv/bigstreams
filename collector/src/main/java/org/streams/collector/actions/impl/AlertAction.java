package org.streams.collector.actions.impl;

import org.streams.collector.actions.CollectorAction;
import org.streams.commons.status.Status;
import org.streams.commons.status.Status.STATUS;

public class AlertAction extends CollectorAction{

	Status status;
	
	public AlertAction(Status status) {
		super();
		this.status = status;
	}

	public void exec() throws Throwable{
		status.setStatus(STATUS.SERVER_ERROR, "Disk full");
	}
	
}
