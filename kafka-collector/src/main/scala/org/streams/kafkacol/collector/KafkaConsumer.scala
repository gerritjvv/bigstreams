package org.streams.kafkacol.collector

import java.util.Properties
import java.util.concurrent.ExecutorService
import org.apache.log4j.Logger
import org.streams.kafkacol.conf.CollectorConfig
import org.streams.streamslog.log.file.FileLogResource
import org.streams.streamslog.log.file.TopicConfig
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.consumer.KafkaStream
import kafka.message.Message
import kafka.serializer.DefaultDecoder
import java.util.concurrent.atomic.AtomicBoolean


class KafkaConsumer(execService:ExecutorService, fileLogResource:FileLogResource, retriesOnError:Int=10) {

  val logger = Logger.getLogger(getClass)
  
  val criticalError = new AtomicBoolean(false)
  
  /**
   * Creates threads for consuming the topics from kafka, each thread will run till the ExecutorService is shutdown.
   */
  def consume(collConf:CollectorConfig) = {
    
       //there for each topic we create a List of KafkaStreams equal in size to the Threads default Threads is 2 as per the TopicConfig
       val kafkaStreamMaps = Consumer.create(collConf.kafkaConfig).createMessageStreams(toThreadMap(collConf.topicConfigs), new DefaultDecoder)
        
       val topicMap = toTopicMap(collConf.topicConfigs)
       
       for((topic, streams) <- kafkaStreamMaps; stream <- streams){
    	   execService.submit(
    	       new Runnable(){ 
    	    	   override def run() {
    	    	     var tries = 0
    	    	     while(!Thread.currentThread().isInterrupted()){
    	    		   try{
    	    		      logger.info("Thread: " + Thread.currentThread() + " consuming from : " + topic)
    	    		     _consume(topicMap(topic), stream)
    	    		     tries = 0
    	    		   }catch{
    	    		     case e:InterruptedException => Thread.currentThread().interrupt(); return
    	    		     case e => logger.error(e.toString(), e)
    	    		   }
    	    		   //sleep a second between errors
    	    		   if(tries >= retriesOnError){
    	    		     criticalError.set(true)
    	    		     logger.error("Retries " + retriesOnError + " exceeded, exiting KafkaConsumer Thread" + Thread.currentThread())
    	    		     return
    	    		   }
    	    		   
    	    		   tries = tries + 1
    	    		     
    	    		   Thread.sleep(1000L)
    	    	     }
    	    	     
    	    	   }
    	    }
    	 )
       }
       
  }
  
  def _consume(topic:TopicConfig, stream:KafkaStream[Message]) = {
    
    val writer = fileLogResource.get(topic.topic)
    val dateParser = topic.tsParser
    
    //for each message in the KafkaStream, extract the date and send the
    //message byte array to the writer
	for (msgAndMeta <- stream){
	    val msg = msgAndMeta.message
	    val offSet = msg.payload.arrayOffset()
	    val untilIndex = offSet + msg.payloadSize
		writer ! ( ( dateParser.dateString(msg), msg.payload.array().slice(offSet, untilIndex) ) )
	}
    	 
    
  }
  
  def toTopicMap(topics:Array[TopicConfig]) = topics.foldLeft(Map[String, TopicConfig]()) { (m, topic:TopicConfig) => m + (topic.topic -> topic) }
  def toThreadMap(topics:Array[TopicConfig]) = topics.foldLeft(Map[String,Int]()) { (m, topic:TopicConfig) => 
    val threads =  if(topic.threads < 1) 1 else topic.threads; 
    m + (topic.topic -> threads) 
  }
  
}

