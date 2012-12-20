package org.streams.streamslog.log.file

import java.io.File
import java.io.FileOutputStream
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import scala.actors.Actor
import org.streams.commons.compression.CompressionPool
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl
import org.streams.commons.status.Status
import org.streams.commons.status.Status.STATUS
import org.streams.commons.compression.CompressionPoolFactory

/**
 * Manages File Writers for topics, each FileWriter in turn manages its files based on date.<br/>
 * 
 */
class FileLogResource(topics: Map[String, TopicConfig]) {

  val openWriters = scala.collection.mutable.Map[String, LogFileWriter]()
  val compressionPoolFactory = new CompressionPoolFactoryImpl(100, 100, NonStatus)

  val execSerivce = Executors.newSingleThreadScheduledExecutor()

  execSerivce.scheduleWithFixedDelay(new RollService(), 100L, 2000L, TimeUnit.MILLISECONDS)

  def get(topic: String) =
    openWriters.getOrElseUpdate(topic, createWriter(topic))

  def createWriter(topic: String) = {
    new LogFileWriter(topics(topic), compressionPoolFactory)
  }

  def close() = {
    execSerivce.shutdownNow()
    for (writer <- openWriters.values) writer ! 'stop
  }

  /**
   * Service that is woken up with a Scheduled Fixed Delay to notify each LogFileWriter to check for files that need rolling.
   */
  class RollService extends Thread {

    override def run() = {
      for (writer <- openWriters.values) writer ! 'checkRolls
    }

  }

}

/**
 * Actor that handles the reading and writing of files determined by date for a topic.<br/>
 * Each file for the topic is itself handled as an Actor wrapped by the FileObj instance.<br/>
 */
class LogFileWriter(topicConfig:TopicConfig, compressionPoolFactory:CompressionPoolFactory) extends Actor {

  val baseDir = topicConfig.baseDir
  val topic = topicConfig.topic
  val compressionPool = compressionPoolFactory.get(topicConfig.codec)
  val extension = topicConfig.codec.getDefaultExtension()
  
  val openFiles = scala.collection.mutable.Map[String, FileObj]()

  start
  
  
  def act() {
    loop {
      react {
        case (date:String, msg:String) =>
          write(date, msg.getBytes())
        case (date: String, msg: Array[Byte]) =>
          write(date, msg)
        case 'stop =>
          closeAll()
          exit('stop)
        case 'checkRolls =>
          checkFilesToRoll()
        case 'flush =>
          flushAll()
        case m:Any =>
          println("Coult no understand: " + m)
      }
    }
  }

  def flushAll() = {
      for((date, fileObj) <- openFiles) fileObj ! 'flush
      
  }
  def checkFilesToRoll() = {
      val rollCheck = topicConfig.rollCheck
	  for((date, fileObj) <- openFiles; if(rollCheck.shouldRoll(fileObj.lastModTime(), fileObj.size()))){
	    fileObj ! 'stop
	    openFiles -= date
	  }
  }

  def write(date: String, msg: Array[Byte]) =
    openFiles.getOrElseUpdate(date, createFile(date)) ! msg

  def createFile(date: String) =
    new FileObj(new File(baseDir, topic + "." + date + "." + System.currentTimeMillis() + extension + "_"), compressionPool)

  def closeAll() =
    for (fileObj <- openFiles.values) fileObj ! 'stop

}

/**
 * Handles the complete life cycle of creating a Compression OutputStream and closing releasing the stream from the CompressionPool.
 */
case class FileObj(file: File, compression: CompressionPool) extends Actor {

  val output = compression.create(new FileOutputStream(file), 1000, TimeUnit.MILLISECONDS)
  var modTs = System.currentTimeMillis()

  
  start()

  def lastModTime() = modTs
  
  def act() {
    loop {
      react {
        case msg: Array[Byte] =>
          <<(msg)
        case 'stop =>
          close()
          exit('stop)
        case 'flush =>
          output.flush()
      }
    }
  }

  def <<(msg: Array[Byte]) = {
    output.write(msg)
    modTs = System.currentTimeMillis()
  }

  def size() = file.length()

  /**
   * Close and rename the file from name_ to name
   */
  def close() = {
    compression.closeAndRelease(output)
    file.renameTo(new File(file.getParentFile(), file.getName().init))
  }

}

object NonStatus extends Status {

  override def setCounter(s: String, c: Int) {}

  def getCounter() = 1

  override def getStatus(): STATUS = null

  override def getStatusMessage() = ""

  override def setStatus(s: STATUS, m: String) {
  }

  override def getStatusTimestamp() = 0L

}