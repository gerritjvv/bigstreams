package org.streams.tools;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.streams.commons.io.Header;
import org.streams.commons.io.Protocol;
import org.streams.commons.io.impl.ProtocolImpl;


/**
 * 
 * This server class is meant for testing the agent.<br/>
 * It will write out the same files as which it receives.<br/>
 */
public class DummyServer {

	private static final Logger LOG = Logger.getLogger(DummyServer.class);

	private static final org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

	public static final String SERVER_PORT = "test.server.port";
	public static final String SERVER_LOGDIR = "test.server.logdir";

	final Map<String, FileWriter> fileMap = new ConcurrentHashMap<String, FileWriter>();

	private ServerBootstrap bootstrap;

	final int port;
	final File logDir;

	Configuration conf;

	public DummyServer(Configuration conf) throws Exception {
		this.conf = conf;

		// set native library path
		if (System.getenv("java.library.path") == null) {

			String path = conf.getString("java.library.path");
			if (path != null) {
				System.setProperty("java.library.path", path);
				Field fieldSysPath = ClassLoader.class
						.getDeclaredField("sys_paths");
				fieldSysPath.setAccessible(true);
				fieldSysPath.set(System.class.getClassLoader(), null);
			} else {
				throw new RuntimeException("java.library.path is not specified");
			}
		}

		port = conf.getInt(SERVER_PORT, 8280);
		logDir = new File(conf.getString(SERVER_LOGDIR, "/tmp/testserver/logs"));

		logDir.mkdirs();

		LOG.info("Using port : " + port);
		LOG.info("Using log dir: " + logDir);

		Runtime.getRuntime().addShutdownHook(new Thread() {

			public void run() {
				shutdown();
			}
		});
	}

	/**
	 * Returns the files that the server created.
	 * 
	 * @return
	 */
	public Set<String> getFilesCreated() {
		return fileMap.keySet();
	}

	public final void shutdown() {

		for (FileWriter writer : fileMap.values()) {
			IOUtils.closeQuietly(writer);
		}

		bootstrap.releaseExternalResources();

		LOG.info("SHUTDOWN Test Server");
	}

	/**
	 * Writes a file name=header.getFileName()(parentDir + fileName)
	 * 
	 * @param header
	 * @param input
	 * @throws Exception
	 */
	protected final void writeFile(Header header, CompressionInputStream input)
			throws Exception {

		LOG.info("HEADER : " + header.toJsonString());

		// File file = new File(header.getFileName());
		// String name = file.getName();
		// String parentDir = file.getParentFile().getName();

		// String key = name;
		//
		// FileWriter writer = fileMap.get(key);
		//
		// if (writer == null) {
		// LOG.info("Creating file for " + key);
		// File fileNew = new File(logDir, key);
		// fileNew.getParentFile().mkdirs();
		// fileNew.createNewFile();
		//
		// writer = new FileWriter(fileNew);
		// fileMap.put(key, writer);
		// }

	}

	@SuppressWarnings("unchecked")
	public static void main(String arg[]) throws Exception {

		if (arg.length != 1) {
			throw new RuntimeException(
					"Please provide 1 argument path  : hadoop style configuration file");
		}

		String confFile = arg[0];

		PropertiesConfiguration props = new PropertiesConfiguration(new File(
				confFile));
		SystemConfiguration sys = new SystemConfiguration();
		CompositeConfiguration cc = new CompositeConfiguration();
		cc.addConfiguration(sys);
		cc.addConfiguration(props);

		Iterator<String> it = props.getKeys();

		while (it.hasNext()) {
			String key = it.next();
			System.setProperty(key, props.getProperty(key).toString());
		}

		DummyServer server = new DummyServer(cc);
		server.connect();

	}

	/**
	 * Startup a ServerBootstrap with NioServerSocketChannelFactory using the
	 * portNo specified in the constructor.
	 * 
	 * @return
	 */
	public ServerBootstrap connect() {

		bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(new MessageFrameDecoder(),
						new TestChannel());
			}
		});

		bootstrap.bind(new InetSocketAddress(port));

		return bootstrap;

	}

	/**
	 * 
	 * Netty channel that receives a request and calls the writeFile method to
	 * write data to a file.
	 */
	class TestChannel extends SimpleChannelHandler {

		Protocol protocol = new ProtocolImpl();

		@Override
		public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
				throws Exception {
			System.out.println("------- Server Channel closed");
			super.channelClosed(ctx, e);
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
				throws Exception {
			try {
				Thread.sleep(1000);

				ChannelBuffer buff = (ChannelBuffer) e.getMessage();

				// System.out.println("Server message received: " +
				// buff.array().length);

				ChannelBufferInputStream channelInput = new ChannelBufferInputStream(
						buff);
				DataInputStream datInput = new DataInputStream(channelInput);

				if (!buff.readable())
					throw new RuntimeException(
							"The channel buffer is not readable");

				// read header
				Header header = protocol.read(conf, datInput);

				CompressionCodec codec = (CompressionCodec) Thread
						.currentThread().getContextClassLoader()
						.loadClass(header.getCodecClassName()).newInstance();

				if (codec instanceof Configurable) {
					((Configurable) codec).setConf(hadoopConf);
				}

				CompressionInputStream compressInput = codec
						.createInputStream(datInput);

				// write file
				// writeFile(header, compressInput);
				LOG.info("Server message available: "
						+ compressInput.available());
				if (!buff.readable())
					throw new RuntimeException(
							" 2 The channel buffer is not readable");

				// write the data from the input stream into the writer.
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(compressInput));
				String line = null;
				int counter = 0;
				while ((line = reader.readLine()) != null) {
					counter++;
					// writer.write(line);
					// writer.write("\n");
				}

				LOG.info("lines read: " + counter);

				ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
				buffer.writeInt(200);

				ChannelFuture future = e.getChannel().write(buffer);
				future.addListener(ChannelFutureListener.CLOSE);

				// super.messageReceived(ctx, e);
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}

		@Override
		public void channelConnected(ChannelHandlerContext ctx,
				ChannelStateEvent e) throws Exception {
			System.out.println("-------- Server Channel connected "
					+ System.currentTimeMillis());
			super.channelConnected(ctx, e);
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
				throws Exception {
			System.out.println("Server Exception Caught");
			e.getCause().printStackTrace();
			e.getChannel().close();
		}

	}

}
