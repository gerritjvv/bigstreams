package org.streams.commons.compression.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.log4j.Logger;
import org.streams.commons.compression.CompressionPool;
import org.streams.commons.status.Status;

/**
 * 
 * Compression Codecs use Decompressor and Compressor instances to do the
 * delagate compression work. Each instance allocates direct memory to spead up
 * memory communication between the native libraries and the java process. Each
 * direc memory allocation will not be automatically released and a release
 * cannot be triggered by the application code. When and how the memory is
 * de-allocated is up to the GC no the JVM version and platform used. This makes
 * the direct allocation of memory very expensive and as a result its better to
 * cache it in a bounded pool.
 * <p/>
 * Thread safety, The Decompressor and Compressor instances are not thread safe,
 * this pool will return an InputStream and OutputStream with a pooled
 * Decompressor or Compressor obtained in a thread safe way.
 * <p/>
 * Pool Sizes: A greater pool size will mean less thread contention but will
 * also mean more memory is allocated.
 * <p/>
 * Non Native Compression Codecs:<br/>
 * Compression Codecs that support non native compression i.e. do not need
 * native libraries and have java alternatives, will not create Compressor(s) or
 * Decompressor(s),<br/>
 * in this case, no resource management is needed and the input and output
 * streams are created directly, ingnoring the pools.
 * <p/>
 * Adaptive compressor/decompressor creation:<br/>
 * The adaptiveIncrement property is set to true by default. <br/>
 * When a caller timesout on waiting for a decompresor or a compressor, a new
 * decompressor/compressor is created<br/>
 * and added to the pool<br/>
 * This means the CompressionPool can grow automatically to create as many<br/>
 * decompressors/compressors as required during peak stages.<br/>
 * At the moment there exists no mechanism for shrinking this pool, doing so<br/>
 * might create a memory leak where decompressors/compressors<br/>
 * are removed and new ones created later on, but beceause the resources for<br/>
 * these are only cleaned on GC no they might stay in memory for<br/>
 * long periods of time.<br/>
 * If a collector runs out of compressors/decompressors you should consider
 * scalling the collector by adding another physical machine.
 */
public class CompressionPoolImpl implements CompressionPool {

	private static final Logger LOG = Logger
			.getLogger(CompressionPoolImpl.class);

	private static final String DECOMPRESSOR_USED_PREF = "DECOMPRESSORS-USED";
	private static final String COMPRESSOR_USED_PREF = "COMPRESSORS-USED";

	private String DECOMPRESSOR_STR;
	private String COMPRESSOR_STR;

	final LinkedBlockingDeque<Decompressor> decompressorQueue;
	final LinkedBlockingDeque<Compressor> compressorQueue;

	final Map<CompressionInputStream, Decompressor> usedDecompressors = new ConcurrentHashMap<CompressionInputStream, Decompressor>();
	final Map<CompressionOutputStream, Compressor> usedCompressors = new ConcurrentHashMap<CompressionOutputStream, Compressor>();

	final CompressionCodec codec;

	boolean hasDecompressors = false;
	boolean hasCompressors = false;

	AtomicInteger compressorsUsedCount = new AtomicInteger();
	AtomicInteger decompressorsUsedCount = new AtomicInteger();

	boolean adaptiveIncrement = true;

	Status status;

	/**
	 * 
	 * @param codec
	 * @param decompressorPoolSize
	 *            a fixed size Decompressor pool is created with size == this
	 *            value
	 * @param compressorPoolSize
	 *            a fixed size Compressor pool is created with size == this
	 *            value
	 */
	public CompressionPoolImpl(CompressionCodec codec,
			int decompressorPoolSize, int compressorPoolSize, Status status) {

		this.codec = codec;
		this.status = status;
		

		DECOMPRESSOR_STR = DECOMPRESSOR_USED_PREF + "-"
				+ codec.getDefaultExtension();
		COMPRESSOR_STR = COMPRESSOR_USED_PREF + "-"
				+ codec.getDefaultExtension();

		if (codec.createDecompressor() != null) {
			hasDecompressors = true;
			LOG.info("Creating " + decompressorPoolSize + " decompressors"
					+ codec.getClass().getName());
			Decompressor[] decompressors = new Decompressor[decompressorPoolSize];
			for (int i = 0; i < decompressorPoolSize; i++) {
				decompressors[i] = codec.createDecompressor();
			}

			decompressorQueue = new LinkedBlockingDeque<Decompressor>(
					Arrays.asList(decompressors));
		} else {
			decompressorQueue = null;
		}

		if (codec.createCompressor() != null) {
			hasCompressors = true;
			LOG.info("Creating " + compressorPoolSize + " Compressors for "
					+ codec.getClass().getName());
			Compressor[] compressors = new Compressor[compressorPoolSize];
			for (int i = 0; i < compressorPoolSize; i++) {
				compressors[i] = codec.createCompressor();
			}

			compressorQueue = new LinkedBlockingDeque<Compressor>(
					Arrays.asList(compressors));
		} else {
			compressorQueue = null;
		}

	}

	@Override
	public CompressionInputStream create(InputStream input, long timeout,
			TimeUnit unit) throws IOException, InterruptedException {
		if (hasDecompressors) {
			Decompressor decompressor = decompressorQueue.poll(timeout, unit);

			if (decompressor == null) {

				if (adaptiveIncrement) {
					LOG.info("Adaptive increment, creating new decompressor");
					decompressor = codec.createDecompressor();
				} else {
					return null;
				}
			}

			CompressionInputStream cin = codec.createInputStream(input,
					decompressor);
			usedDecompressors.put(cin, decompressor);
			status.setCounter(DECOMPRESSOR_STR,
					decompressorsUsedCount.getAndIncrement());
			return cin;

		} else {
			return codec.createInputStream(input);
		}
	}

	@Override
	public CompressionOutputStream create(OutputStream output, long timeout,
			TimeUnit unit) throws IOException, InterruptedException {
		if (hasCompressors) {
			Compressor compressor = compressorQueue.poll(timeout, unit);
			if (compressor == null) {
				if (adaptiveIncrement) {
					LOG.info("Adaptive increment, creating new compressor");
					compressor = codec.createCompressor();
				} else {
					return null;
				}
			}

			CompressionOutputStream cout = codec.createOutputStream(output,
					compressor);
			usedCompressors.put(cout, compressor);
			status.setCounter(COMPRESSOR_STR,
					compressorsUsedCount.getAndIncrement());
			return cout;

		} else {
			return codec.createOutputStream(output);
		}
	}

	@Override
	public void closeAndRelease(CompressionInputStream cin) {

		IOUtils.closeQuietly(cin);

		if (hasDecompressors) {
			Decompressor dec = usedDecompressors.remove(cin);
			dec.reset();
			decompressorQueue.offer(dec);
			status.setCounter(DECOMPRESSOR_STR,
					decompressorsUsedCount.decrementAndGet());
		}

	}

	@Override
	public void closeAndRelease(CompressionOutputStream cout) {

		try {
			// finish quietly
			cout.finish();
		} catch (IOException ioexp) {
			LOG.error(ioexp.toString(), ioexp);
		}

		IOUtils.closeQuietly(cout);

		if (hasCompressors) {
			Compressor comp = usedCompressors.remove(cout);
			comp.reset();
			compressorQueue.offer(comp);
			status.setCounter(COMPRESSOR_STR,
					compressorsUsedCount.decrementAndGet());
		}

	}

	public boolean isAdaptiveIncrement() {
		return adaptiveIncrement;
	}

	public void setAdaptiveIncrement(boolean adaptiveIncrement) {
		this.adaptiveIncrement = adaptiveIncrement;
	}

}
