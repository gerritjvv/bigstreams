package org.streams.kafkacol.collector

import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.log4j.Logger
import org.streams.kafkacol.conf.CollectorConfig.apply
import org.streams.streamslog.log.file.FileLogResource
import org.streams.kafkacol.conf.CollectorConfig

import joptsimple.OptionParser

/**
 * Kafka Collector application
 */
object KafkaCollector{

  val logger = Logger.getLogger(getClass)
  
  val parser = new OptionParser(){
      accepts("config").withRequiredArg().ofType(classOf[File])
      .describedAs("configuration directory")
    };
    
    
  def main(args:Array[String]):Unit = {
    
    try{
    	val options = parser.parse(args:_*)
    	val configDir = options.valueOf("config").asInstanceOf[File]
    	
    	runApp(configDir)
    	
    	
    }catch{
      case e => 
        	logger.error(e.toString(), e);
        	e.printStackTrace()
            parser.printHelpOn(System.out)
            System.exit(-1)
    }
    
    System.exit(0)
  }
  
  def runApp(configDir:File) = {
    val collectorConf = CollectorConfig(configDir)
    val execService = Executors.newCachedThreadPool()
    val fileLogResource = new FileLogResource(collectorConf.topicMap, collectorConf.compressorCount)
    
    try{
      
      val kafkaConsumer = new KafkaConsumer(execService, fileLogResource)
      kafkaConsumer.consume(collectorConf)
      
      logger.info("Consumption started, waiting for shutdown...")
      while(!(Thread.currentThread().isInterrupted() || kafkaConsumer.criticalError.get()))
    	   Thread.sleep(1000L)
      	   
    }catch{
      case e:InterruptedException => logger.info("closing")
      case e => logger.error(e.toString(), e)
    }finally{
      fileLogResource.close
      execService.shutdown()
      logger.info("waiting for shutdown")
      if(!execService.awaitTermination(20, TimeUnit.SECONDS)){
        logger.warn("forcing shutdown")
        for(th <- execService.shutdownNow())
          logger.warn("Forced shutdown for thread: " + th)
      }
      
    }
    
    logger.info("bye")
  }
  
  def loadCollectorConf(configDir:File) = CollectorConfig(configDir)
  
  
}