package org.streams.test.agent.send;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.streams.agent.file.FileTrackingStatus;
import org.streams.agent.mon.status.AgentStatus;
import org.streams.agent.mon.status.impl.AgentStatusImpl;
import org.streams.agent.send.FileSendTask;
import org.streams.agent.send.impl.FilesSendWorkerImpl;
import org.streams.agent.send.impl.FilesToSendQueueImpl;
import org.streams.agent.send.utils.MapTrackerMemory;

/**
 * 
 * This test is important enough to be apart.<br/>
 * It tests that when a file is being sent and the directory checking thread
 * adds this file<br/>
 * again to the queue as updated, another thread i.e. part of the FileSendWorker
 * does not start sending<br/>
 * this file.<br/>
 * 
 */
public class TestFileSendWorkerMultiUpdatesConflict {

	@Test
	public void testConflic() throws InterruptedException {
		final MapTrackerMemory memory = new MapTrackerMemory();
		final int threadCount = 10;
		
		//fill memory with one file
		FileTrackingStatus fileToSendStatus = createFileTrackingStatus(1000);
		memory.updateFile(fileToSendStatus);
		
		final FilesToSendQueueImpl queue = new FilesToSendQueueImpl(memory);

		final AgentStatus agentStatus = new AgentStatusImpl();

		//FileTrackingStatus items will be added here depending on their file size.
		final Map<Long, FileTrackingStatus> statusMap = new ConcurrentHashMap<Long, FileTrackingStatus>();
		
		//so this integer will be incremented every time 
		final AtomicInteger conflictCount = new AtomicInteger(0);
		final AtomicInteger threadCounter = new AtomicInteger(threadCount);
		
		//we use a latch to know when all items have been sent.
		final CountDownLatch filesSentLatch = new CountDownLatch(threadCount);
		
		//a file task that if an item is detected in the fileSentItems set 
		//a conflict is registered by incrementing the conflictCount variable.
		final FileSendTask fileSendTask = new FileSendTask() {

			@Override
			public void sendFileData(FileTrackingStatus fileStatus)
					throws IOException {

				System.out.println("new FileSendworker: " + threadCounter.get());
				//we use this to asynch add a FileTrackingStatus to the memory, while
				//the queue has already read and sent a FileTrackingStatus.
				if(threadCounter.getAndDecrement() > 0){
					FileTrackingStatus fileToSendStatus = createFileTrackingStatus(1000);
					memory.updateFile(fileToSendStatus);
				}
				
				try {
					Thread.sleep(100L);
				} catch (InterruptedException e1) {
					return;
				}
				
				if(statusMap.containsKey(fileStatus.getFileSize())){
					//if this happens it means another thread has seen the above sync update
					//and is also reading the file
					conflictCount.incrementAndGet();
				}else{
					//add, wait a second and remove
					statusMap.put(fileStatus.getFileSize(), fileStatus);
					try {
						Thread.sleep(200L);
					} catch (InterruptedException e) {
						return;
					}
					statusMap.remove(fileStatus.getFileSize());
				}
				
				
				filesSentLatch.countDown();
			}

		};

		// now we simulate
		// sending multiple items.
		ExecutorService threadService = Executors.newFixedThreadPool(threadCount);
		for(int i = 0; i < threadCount; i++){
			threadService.submit(
					new FilesSendWorkerImpl(queue, agentStatus, memory, fileSendTask)
			);
		}
		
		//wait until the tracker memory is empty.
		filesSentLatch.await();
		threadService.shutdown();
		
		
		assertEquals(0, conflictCount.get());
		
	}

	/**
	 * file size is used as a unique identifier
	 * @param fileSize
	 * @return
	 */
	private FileTrackingStatus createFileTrackingStatus(int fileSize) {

		FileTrackingStatus fileToSendStatus = new FileTrackingStatus();
		fileToSendStatus.setPath("testfile");
		fileToSendStatus.setFileSize(fileSize);
		fileToSendStatus.setLogType("Test");
		fileToSendStatus.setStatus(FileTrackingStatus.STATUS.READY);
		fileToSendStatus.setLastModificationTime(10000L);

		return fileToSendStatus;
	}
}
