package org.streams.agent.mon.impl;

import java.util.Collection;

import javax.inject.Inject;
import javax.inject.Named;

import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;
import org.streams.agent.file.actions.FileLogActionEvent;
import org.streams.agent.file.actions.FileLogManagerMemory;

/**
 * Shows the state of all events in the FileLogManagerMemory
 */
@Named("FileLogActionManagerResource")
public class FileLogActionManagerResource extends ServerResource {

	FileLogManagerMemory memory;

	public FileLogActionManagerResource() {
	}

	@Get("json")
	public Collection<FileLogActionEvent> list() throws ResourceException {

		return memory.listEvents();

	}

	public FileLogManagerMemory getMemory() {
		return memory;
	}

	@Inject
	public void setMemory(FileLogManagerMemory memory) {
		this.memory = memory;
	}

}
