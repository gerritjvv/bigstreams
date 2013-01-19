package org.streams.kafkacol

import java.io.File
import java.io.FileInputStream
import java.util.Properties
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import kafka.producer.ProducerData
import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import org.streams.streamslog.log.file.FileLogResource
import org.streams.kafkacol.conf.CollectorConfig
import java.util.concurrent.Executors
import kafka.consumer.ConsumerConfig
import org.streams.kafkacol.collector.KafkaConsumer

@RunWith(classOf[JUnitRunner])
class TestKafkaConsumer extends FlatSpec with ShouldMatchers{

  
  def zkPath = new File("target/test/testKafkaConsumer/" + System.currentTimeMillis())

  if (zkPath.exists()) FileUtils.deleteDirectory(zkPath)
  zkPath.mkdirs()

  def zkDataPath = new File(zkPath, "data")
  zkDataPath.mkdirs()
  def zkLogPath = new File(zkPath, "log")
  zkLogPath.mkdirs()

  val zkEmbedded = new EmbeddedZookeeper(zkDataPath, zkLogPath)
  
  val serverProps = new Properties()
  serverProps.load(new FileInputStream("src/test/resources/server.properties"))
  val server = new KafkaServer(new KafkaConfig(serverProps))
  server.startup

  val producerProps = new Properties()
  producerProps.load(new FileInputStream("src/test/resources/producer.properties"))
  
  val topics = Array("test1", "test2")
  
  for(topic <- topics) 
    KafkaProducer.send(topic, 10)
  
  val consumerProps = new Properties()
  consumerProps.load(new FileInputStream("src/test/resources/kafka.properties"))
  val kafkaConfig = new ConsumerConfig(consumerProps)
  
  
  //create output dir
  val outputDir = new File("target/test/testKafkaConsumer/consumer" + System.currentTimeMillis())
 
  val collectorConf = CollectorConfig.getTestConfig(topics, kafkaConfig, outputDir, 1)
  val execService = Executors.newCachedThreadPool()
  val fileLogResource = FileLogResource(collectorConf.topicMap, collectorConf.compressorCount)
  
  
  val kafkaConsumer = new KafkaConsumer(execService, fileLogResource)
  
  
  "Consumer" should "read messages" in {
    kafkaConsumer.consume(collectorConf)
    
    Thread.sleep(5000L)
      
    fileLogResource.close
    
    assert(kafkaConsumer.criticalError.get() == false)
  }
  
}


object KafkaProducer{
  
  val props = new Properties()
  props.load(new FileInputStream("src/test/resources/producer.properties"))
    
  def send(topic:String, n:Int) = {
    
    val producer = new Producer[String, String](new ProducerConfig(props))
    try{
    	0 until n foreach { i => producer.send(new ProducerData[String, String](topic, "TestMessage")) }
    }finally{
    	producer.close
    }
  }
  
}


