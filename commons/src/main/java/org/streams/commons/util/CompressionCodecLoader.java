package org.streams.commons.util;

import java.lang.reflect.Field;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * 
 * A utility function that will load the CompressionCodec.<br/>
 * The utility will also load the java.library.path before trying to load the
 * codec.<br/>
 * The java.library.path may be specified as en environment variable, on the
 * classpath or in the configuration files.<br/>
 * 
 * 
 */
public class CompressionCodecLoader {

	/**
	 * Loads the correct CompressionCodec class
	 * 
	 * @param conf
	 * @param codecClass
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws NoSuchFieldException
	 * @throws SecurityException
	 */
	public final static CompressionCodec loadCodec(Configuration configuration,
			String codecClass) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, SecurityException,
			NoSuchFieldException {

		if (System.getenv("java.library.path") == null) {

			String path = configuration.getString("java.library.path");
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

		CompressionCodec codec = (CompressionCodec) Thread.currentThread()
				.getContextClassLoader().loadClass(codecClass).newInstance();

		if (codec instanceof Configurable) {
			((Configurable) codec)
					.setConf(new org.apache.hadoop.conf.Configuration());
		}

		return codec;
	}

	
}
