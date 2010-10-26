package org.streams.gring.group.error;

import org.streams.gring.group.MemberDesc;

/**
 * 
 * Thrown when a current member encounters an Exception while transmitting a
 * Message to this Member.
 * 
 */
public class GRingMemberComException extends RuntimeException {

	MemberDesc member;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public GRingMemberComException(MemberDesc member) {
		super();
		this.member = member;
	}

	public GRingMemberComException(MemberDesc member, String message, Throwable cause) {
		super(message, cause);
		this.member = member;
	}

	public GRingMemberComException(MemberDesc member, String message) {
		super(message);
		this.member = member;
	}


	public MemberDesc getMember() {
		return member;
	}
}
