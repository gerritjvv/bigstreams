package org.streams.commons.util;

import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * 
 * Applies a consistent hash bucketing scheme.<br/>
 * I.e. N buckets are created e.g. 1 - 512.  <br/>
 * Key presented to the methods of this instance are consistently assigned a bucket.
 */
public class ConsistentHashBuckets {

	int totalBuckets = 10;
	int keyRange = Integer.MAX_VALUE;
	
	final SortedMap<Integer,SortedSet<Integer>> buckets;
	
	/**
	 * Create 256 buckets
	 */
	public ConsistentHashBuckets() {
		buckets = createBuckets();
	}
	
	/**
	 * 
	 * @param totalBuckets
	 */
	public ConsistentHashBuckets(int totalBuckets) {
		super();
		this.totalBuckets = totalBuckets;
		buckets = createBuckets();
	}
	/**
	 * 
	 * @param totalBuckets
	 */
	public ConsistentHashBuckets(int totalBuckets, int keyRange) {
		super();
		this.totalBuckets = totalBuckets;
		this.keyRange = keyRange;
		buckets = createBuckets();
	}

	private final SortedMap<Integer,SortedSet<Integer>>createBuckets(){
		int val = (int)(keyRange / totalBuckets);
		
		SortedMap<Integer,SortedSet<Integer>> bucketMap = new TreeMap<Integer, SortedSet<Integer>>();
		
		
		
		int space = val;
		for(int i = 0; i < totalBuckets; i++){
			int hashcode = new Integer(space).hashCode();
			if(hashcode < 0) hashcode *= -1;
			
			SortedSet<Integer> set = new TreeSet<Integer>();
			
			int innerBucketTotal = 512;
			int innerVal = hashcode/innerBucketTotal;
			int innerSpace = innerVal;
			for(int a = 0; a < innerBucketTotal; a++){
				int innerhashcode = new Integer(innerSpace).hashCode();
				if(innerhashcode < 0) innerhashcode *= -1;
				set.add(innerhashcode);
				innerSpace += innerVal;
			}
			
			bucketMap.put(hashcode, set);
			space += val;
		}
		
		
		return bucketMap;
	}

	/**
	 * For the key, consistently return the same bucket.
	 * @param key
	 * @return
	 */
	public Integer getBucket(String key){
		int hashcode = key.hashCode();
		if(hashcode < 0) hashcode *= -1;
		
		Integer mapKey = buckets.tailMap(hashcode).firstKey();
		return buckets.get(mapKey).tailSet(hashcode).first();
	}
	
}
