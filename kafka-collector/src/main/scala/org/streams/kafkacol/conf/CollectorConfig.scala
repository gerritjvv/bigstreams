package org.streams.kafkacol.conf

import java.io.File
import org.streams.streamslog.log.file.TopicConfig
import org.streams.streamslog.log.file.TopicConfigParser
import java.io.FileInputStream
import kafka.consumer.ConsumerConfig
import org.streams.streamslog.log.file.NowMessageTimeParser
import org.streams.streamslog.log.file.DateSizeCheck
import org.apache.hadoop.io.compress.GzipCodec

case class CollectorConfig(topicConfigs: Array[TopicConfig], topicMap: Map[String, TopicConfig], kafkaConfig: ConsumerConfig,
    retriesOnError:Int=10, replayWAL:Boolean=true, httpPort:Int=7001) {

  /**
   * We potentially need as many compressors as there are topics plus a cache
   */
  val compressorCount = topicConfigs.size + 50

}

object CollectorConfig {

  def getTestConfig(topics: Array[String], kafkaConfig: ConsumerConfig, outputDir: File, threads: Int) = {
    val topicConfigs = topics.map { topic =>
      new TopicConfig(topic, org.streams.streamslog.log.file.DefaultMessageMetaDataParser, new DateSizeCheck(1000, 100), new GzipCodec(),
        outputDir, false, true, threads)
    }

    new CollectorConfig(topicConfigs, toTopicMap(topicConfigs), kafkaConfig)
  }

  def apply(baseDir: File) = {

    val topicsFile = new File(baseDir, "/topics")
    if (!(topicsFile.exists() && topicsFile.canRead()))
      throw new RuntimeException(topicsFile.getAbsolutePath() + " does not exist or is not readable")

    val topics = TopicConfigParser(topicsFile)
    val props = loadProps(baseDir)
    
    new CollectorConfig(topics, toTopicMap(topics), new ConsumerConfig(props), props.getProperty("httpPort", "7001").toInt)
  }

  def toTopicMap(topics: Array[TopicConfig]) = topics.foldLeft(Map[String, TopicConfig]()) { (m, config) => m + (config.topic -> config) }

  def loadProps(configDir: File) = {
    val file = new File(configDir, "kafka.properties")
    if (!(file.exists() && file.canRead()))
      throw new RuntimeException(configDir.getAbsolutePath() + "/kafka.properties does not exist or is not readable")

    val props = new java.util.Properties
    props.load(new FileInputStream(file))
    props
  }

}