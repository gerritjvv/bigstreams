package org.streams.coordination.file.history.impl.hazelcast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.streams.coordination.file.history.FileTrackerHistoryItem;
import org.streams.coordination.file.history.FileTrackerHistoryMemory;

import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

/**
 * 
 * Provides the hazelcast IMap as the underlying storage.
 * 
 */
public class FileTrackerHistoryMemoryImpl implements FileTrackerHistoryMemory {

	private static final Logger LOG = Logger
			.getLogger(FileTrackerHistoryMemoryImpl.class);

	MultiMap<String, FileTrackerHistoryItem> map;
	IMap<String, FileTrackerHistoryItem> latestStatusMap;
	
	ExecutorService service = null;

	/**
	 * @param map Multi map from hazelcast to use
	 * @param map that will hold the latest status
	 * @param threads number of threads to use to serve the async history put.
	 */
	public FileTrackerHistoryMemoryImpl(
			MultiMap<String, FileTrackerHistoryItem> map, IMap<String, FileTrackerHistoryItem> latestStatusMap, int threads) {
		this.map = map;
		this.latestStatusMap = latestStatusMap;
		service = Executors.newFixedThreadPool(threads);
		
	}
	
	public void close(){
		service.shutdown();
	}

	/**
	 * The put is async and will never throw an error.
	 */
	public Future<?> addAsyncToHistory(final FileTrackerHistoryItem item){
		return service.submit(new Runnable() {
			public void run() {
				try {
					map.put(item.getAgent(), item);
					latestStatusMap.put(item.getAgent(), item);
				} catch (Throwable t) {
					LOG.error(t.toString(), t);
				}
			}
		});
	}
	
	/**
	 * 
	 */
	@Override
	public void addToHistory(final FileTrackerHistoryItem item) {
		map.put(item.getAgent(), item);
		latestStatusMap.put(item.getAgent(), item);
	}

	@Override
	public Collection<FileTrackerHistoryItem> getAgentHistory(String agent,
			int from, int max) {
		return trimDown(map.get(agent), from, max);

	}

	@Override
	public Map<String, FileTrackerHistoryItem> getLastestAgentStatus() {
		//the file tracker history item is sorted by date already.
		return latestStatusMap;
	}

	@Override
	public Map<String, Collection<FileTrackerHistoryItem>> getLastestCollectorStatus() {
		Map<String, Collection<FileTrackerHistoryItem>> collectorHistory = new HashMap<String, Collection<FileTrackerHistoryItem>>();
		
		
		for(Entry<String, FileTrackerHistoryItem> entry : latestStatusMap.entrySet()){
			
			FileTrackerHistoryItem item = entry.getValue();
			
			Collection<FileTrackerHistoryItem> coll = collectorHistory.get(item.getCollector());
			if(coll == null){
				coll = new ArrayList<FileTrackerHistoryItem>();
				collectorHistory.put(item.getCollector(), coll);
			}
			
			coll.add(item);
			
		}
		
		return collectorHistory;
	}

	@Override
	public int getAgentHistoryCount(String agentName) {
		Collection<FileTrackerHistoryItem> history = map.get(agentName);
		return (history == null) ? 0 : history.size();
	}

	@Override
	public int deleteAgentHistory(String agentName) {
		Collection<FileTrackerHistoryItem> history = map.remove(agentName);
		latestStatusMap.remove(agentName);
		
		return (history == null) ? 0 : history.size();
	}

	/**
	 * Ensures that the collection is within the from max limits.<br/>
	 * If it is no changes are applied. If not a new collection is created.
	 * 
	 * @param <T>
	 * @param baseColl
	 * @param from
	 * @param max
	 * @return Collection
	 */
	private static final <T> Collection<T> trimDown(Collection<T> baseColl,
			int from, int max) {
		Collection<T> coll = null;

		if (baseColl != null) {
			if (max >= baseColl.size()) {
				coll = baseColl;
			} else {

				// use Collections when ever possible

				// manually trim down the collection
				// so that the from max parameters are respected
				Iterator<T> it = baseColl.iterator();
				int index = 0;
				coll = new ArrayList<T>(max);

				while (it.hasNext()) {
					if (index >= from) {
						if ((index - from) < max)
							coll.add(it.next());
						else
							// if max reached break
							break;
					}
					index++;
				}

			}
		}

		return coll;
	}
}
