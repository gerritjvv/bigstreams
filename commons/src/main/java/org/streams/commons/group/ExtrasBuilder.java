package org.streams.commons.group;

import java.util.List;

import org.streams.commons.group.Group.GroupStatus.ExtraField;

/**
 * Allows the extras column to be built by outside code that is not known in
 * advance.
 * 
 */
public interface ExtrasBuilder {

	/**
	 * Create an instance of GroupStatus.Extras Allowed to return null
	 * 
	 * @return GroupStatus.Extras or null
	 */
	List<ExtraField> build();

}
