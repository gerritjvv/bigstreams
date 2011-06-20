package org.streams.commons.compression;

import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * 
 * Manages instances of CompressionPool(s) for different CompressionCodec(s)
 */
public interface CompressionPoolFactory {

	CompressionPool get(CompressionCodec codec);

}
