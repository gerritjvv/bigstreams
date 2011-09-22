package org.streams.test.commons.util;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.streams.commons.util.ConsistentHashBuckets;

public class ConsistentHashBucketsTest {

	/**
	 * Tests the hash spread against a known value.<br/>
	 * i.e. for the fixed values presented the maximum items in a bucket cannot exceed more than N,
	 * where N is a known value e.g 100.</br/>
	 */
	@Test
	public void testHashSpread(){
		
		//create file names with dates and hours for 1 month
		List<String> fileNames = new ArrayList<String>();
		for(int months = 0; months < 12; months++){
		for(int days =1; days < 31; days++){
			for(int hours = 0; hours < 24; hours++){
				fileNames.add("myfile_ "  + "2011-" + months + "-" + days + "-" + hours + ".txt");
				fileNames.add("a_ "  + "2011-" + months + "-" + days + "-" + hours + ".txt");
				fileNames.add("z_ "  + "2011-" + months + "-" + days + "-" + hours + ".txt");
				fileNames.add("e_ "  + "2011-" + months + "-" + days + "-" + hours + ".txt");
			}
		}
		}
		
		ConsistentHashBuckets buckets = new ConsistentHashBuckets();
		Map<Integer, AtomicInteger> bucketMap = new HashMap<Integer, AtomicInteger>();
		
		int hashMax = 0;
		int hashMin = Integer.MAX_VALUE;
		
		for(String fileName : fileNames){
			int hash = fileName.hashCode();
			hashMax = Math.max(hashMax, hash);
			hashMin = Math.min(hashMin, hash);
			Integer bucket = buckets.getBucket(fileName);
			AtomicInteger counts = bucketMap.get(bucket);
			if(counts == null){
				counts = new AtomicInteger(1);
				bucketMap.put(bucket, counts);
			}else{
				counts.incrementAndGet();
			}
			
		}
		
		
		int max = 0;
		for(Integer key : bucketMap.keySet()){
			max = Math.max(max, bucketMap.get(key).get());
			System.out.println(key + ": " + bucketMap.get(key).get());
		}
		
		System.out.println("Max: " + max);
		assertTrue(max < 100);
	}
	
}
