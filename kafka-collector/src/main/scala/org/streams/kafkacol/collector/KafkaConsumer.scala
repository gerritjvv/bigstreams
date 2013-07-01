package org.streams.kafkacol.collector

import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import org.I0Itec.zkclient.IZkStateListener
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.Logger
import org.streams.kafkacol.conf.CollectorConfig
import org.streams.streamslog.log.file.FileLogResource
import org.streams.streamslog.log.file.TopicConfig
import akka.actor.actorRef2Scala
import kafka.consumer.Consumer
import kafka.consumer.KafkaStream
import kafka.message.Message
import kafka.serializer.DefaultDecoder
import kafka.utils.ZKStringSerializer
import org.apache.zookeeper.Watcher.Event.KeeperState

class KafkaConsumer(execService: ExecutorService, fileLogResource: FileLogResource, retriesOnError: Int = 10) {

  val logger = Logger.getLogger(getClass)

  val criticalError = new AtomicBoolean(false)
  val nonCriticalError = new AtomicBoolean(false)
  val error = new AtomicReference[Throwable]()

  val meterRequests = Metrics.meter("requests")
  val bytesRead = Metrics.histogram("bytesread")

  /**
   * Creates threads for consuming the topics from kafka, each thread will run till the ExecutorService is shutdown.
   */
  def consume(collConf: CollectorConfig) = {

    //there for each topic we create a List of KafkaStreams equal in size to the Threads default Threads is 2 as per the TopicConfig
    val kafkaStreamMaps = Consumer.create(collConf.kafkaConfig).createMessageStreams(toThreadMap(collConf.topicConfigs), new DefaultDecoder)

    if (collConf.kafkaConfig.zkConnect != null) {
      val zkClient = new ZkClient(collConf.kafkaConfig.zkConnect,
        collConf.kafkaConfig.zkSessionTimeoutMs,
        collConf.kafkaConfig.zkConnectionTimeoutMs,
        ZKStringSerializer)
      zkClient.subscribeStateChanges(new IZkStateListener() {
        def handleStateChanged(state: KeeperState) = {
          nonCriticalError.set(
            state.equals(KeeperState.Disconnected) ||
              state.equals(KeeperState.Unknown))
        }

        def handleNewSession() = {}

      })

    }

    val topicMap = toTopicMap(collConf.topicConfigs)

    for ((topic, streams) <- kafkaStreamMaps; stream <- streams) {
      execService.submit(
        new Runnable() {
          override def run() {
            var tries = 0
            while (!Thread.currentThread().isInterrupted()) {
              try {
                logger.info("Thread: " + Thread.currentThread() + " consuming from : " + topic)
                _consume(topicMap(topic), stream)
                tries = 0
              } catch {
                case e: InterruptedException =>
                  Thread.currentThread().interrupt(); return
                case e => {
                  logger.error(e.toString(), e)
                  error.set(e)
                }
              }
              //sleep a second between errors
              if (tries >= retriesOnError) {
                criticalError.set(true)
                logger.error("Retries " + retriesOnError + " exceeded, exiting KafkaConsumer Thread" + Thread.currentThread())
                return
              }

              tries = tries + 1

              Thread.sleep(1000L)
            }

          }
        })
    }

  }

  def _consume(topic: TopicConfig, stream: KafkaStream[Message]) = {

    val writer = fileLogResource.get(topic.topic)
    // create a new copy of the script parser only if its not threadsafe
    val msgParser = if (topic.metaDataParser.isThreadSafe) topic.metaDataParser else topic.metaDataParser.clone()
    //for each message in the KafkaStream, extract the date and send the
    //message byte array to the writer

    for (msgAndMeta <- stream) {
      val msgRaw = msgAndMeta.message
      val offSet = msgRaw.payload.arrayOffset()
      val untilIndex = offSet + msgRaw.payloadSize

      val msg = msgRaw.payload.array().slice(offSet, untilIndex)
      writer ! msgParser.parse(topic.topic, msg)

      //add to emtrics counters
      meterRequests.mark()
      bytesRead.update(msgRaw.payload.array().size)

    }

  }

  def toTopicMap(topics: Array[TopicConfig]) = topics.foldLeft(Map[String, TopicConfig]()) { (m, topic: TopicConfig) => m + (topic.topic -> topic) }
  def toThreadMap(topics: Array[TopicConfig]) = topics.foldLeft(Map[String, Int]()) { (m, topic: TopicConfig) =>
    val threads = if (topic.threads < 1) 1 else topic.threads;
    m + (topic.topic -> threads)
  }

}

