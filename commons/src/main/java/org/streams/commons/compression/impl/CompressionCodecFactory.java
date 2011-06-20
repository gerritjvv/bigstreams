package org.streams.commons.compression.impl;

import java.io.File;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.jboss.netty.util.internal.ConcurrentHashMap;

/**
 * 
 * Load compression codecs based on the codecs property
 *
 */
public class CompressionCodecFactory {
	
	Map<String, CompressionCodec> map = new ConcurrentHashMap<String, CompressionCodec>();
	
	@SuppressWarnings("unchecked")
	public CompressionCodecFactory(Collection<String> codecFileMapping) throws ClassNotFoundException, InstantiationException, IllegalAccessException{
		
		
		for(String mapping : codecFileMapping){
			String[] split = mapping.split(":");
			
			if(split.length != 2){
				throw new IllegalArgumentException("Mapping must have format prefix:codec");
			}
			
			Class<? extends CompressionCodec> codec = (Class<? extends CompressionCodec>) Thread.currentThread().getContextClassLoader().loadClass(split[1]);
			
			map.put(split[1], codec.newInstance());
		}
		
	}
	
	
	public CompressionCodec getCodec(File file){
		return map.get(FilenameUtils.getExtension(file.getName()));
	}
	
	
}
