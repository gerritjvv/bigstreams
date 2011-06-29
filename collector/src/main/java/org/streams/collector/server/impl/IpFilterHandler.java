package org.streams.collector.server.impl;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChannelUpstreamHandler;

/**
 * 
 * Uses to block certain ip requests.
 * 
 * 
 */
public class IpFilterHandler implements ChannelUpstreamHandler {

	private static final Logger LOG = Logger.getLogger(IpFilterHandler.class);
	
	Set<String> blockedIps = new TreeSet<String>();

	/**
	 * Called when the channel is connected. It returns True if the
	 * corresponding connection is to be allowed. Else it returns False.
	 * 
	 * @param ctx
	 * @param e
	 * @param inetSocketAddress
	 *            the remote {@link InetSocketAddress} from client
	 * @return True if the corresponding connection is allowed, else False.
	 * @throws Exception
	 */
	boolean accept(ChannelHandlerContext ctx, ChannelEvent e,
			InetSocketAddress inetSocketAddress) throws Exception {
		return !blockedIps.contains(inetSocketAddress.getAddress()
				.getHostAddress());
	}

	/**
	 * Internal method to test if the current channel is blocked. Should not be
	 * overridden.
	 * 
	 * @param ctx
	 * @return True if the current channel is blocked, else False
	 */
	protected boolean isBlocked(ChannelHandlerContext ctx) {
		return ctx.getAttachment() != null;
	}

	protected boolean continues(ChannelHandlerContext ctx, ChannelEvent e)
			throws Exception {
		return false;
	}

	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
			throws Exception {
		if (e instanceof ChannelStateEvent) {
			ChannelStateEvent evt = (ChannelStateEvent) e;
			switch (evt.getState()) {
			case OPEN:
			case BOUND:
				// Special case: OPEND and BOUND events are before CONNECTED,
				// but CLOSED and UNBOUND events are after DISCONNECTED: should
				// those events be blocked too?
				if (isBlocked(ctx) && !continues(ctx, evt)) {
					// don't pass to next level since channel was blocked early
					return;
				} else {
					ctx.sendUpstream(e);
					return;
				}
			case CONNECTED:
				if (evt.getValue() != null) {
					// CONNECTED
					InetSocketAddress inetSocketAddress = (InetSocketAddress) e
							.getChannel().getRemoteAddress();
					if (!accept(ctx, e, inetSocketAddress)) {
						ctx.setAttachment(Boolean.TRUE);
						
						final ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
						
						buffer.writeBytes("/".getBytes());
						buffer.writeBytes("ok".getBytes());
						
						ChannelFuture future = e.getChannel().write(buffer);
						future.addListener(ChannelFutureListener.CLOSE);
						LOG.info("Ignoring " + inetSocketAddress.getAddress().getHostAddress());
						
						if (isBlocked(ctx) && !continues(ctx, evt)) {
							// don't pass to next level since channel was
							// blocked early
							return;
						}
					}
					// This channel is not blocked
					ctx.setAttachment(null);
				} else {
					// DISCONNECTED
					if (isBlocked(ctx) && !continues(ctx, evt)) {
						// don't pass to next level since channel was blocked
						// early
						return;
					}
				}
				break;
			}
		}
		if (isBlocked(ctx) && !continues(ctx, e)) {
			// don't pass to next level since channel was blocked early
			return;
		}
		// Whatever it is, if not blocked, goes to the next level
		ctx.sendUpstream(e);
	}

	public Set<String> getBlockedIps() {
		return blockedIps;
	}

	public void setBlockedIps(Set<String> blockedIps) {
		this.blockedIps = blockedIps;
	}

}
