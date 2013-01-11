package org.streams.streamslog.log.file

import java.io.File
import java.io.FileOutputStream
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.actors.Actor
import scala.collection.JavaConversions._
import org.apache.commons.codec.binary.Base64
import org.apache.log4j.Logger
import org.streams.commons.compression.CompressionPool
import org.streams.commons.compression.CompressionPoolFactory
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl
import org.streams.commons.status.Status
import org.streams.commons.status.Status.STATUS
import org.streams.streamslog.jmx.JMXHelpers._
import java.util.concurrent.ConcurrentHashMap

/**
 * Manages File Writers for topics, each FileWriter in turn manages its files based on date.<br/>
 *
 */
class FileLogResource(topics: Map[String, TopicConfig], compressors: Int = 100) {

  val logger = Logger.getLogger(classOf[FileLogResource])
  val statusActor = new StatusActor
  
  //check that all directories do exist
  for (topic <- topics.values) {
    if (!topic.baseDir.exists()) topic.baseDir.mkdirs()

    if (!(topic.baseDir.exists() && topic.baseDir.canWrite()))
      throw new RuntimeException("The directory " + topic.baseDir.getAbsolutePath() + " does not exist or is not writable")
  }

  val openWriters = new ConcurrentHashMap[String, LogFileWriter]()
  val compressionPoolFactory = new CompressionPoolFactoryImpl(compressors, compressors, NonStatus)

  val execSerivce = Executors.newScheduledThreadPool(2)
  
  execSerivce.scheduleWithFixedDelay(new RollService(), 10L, 2L, TimeUnit.SECONDS)
  execSerivce.scheduleWithFixedDelay(new StatusPrintService(statusActor), 10L, 10L, TimeUnit.SECONDS)

  def get(topic: String) = {
    openWriters.synchronized{
     val writer = if(openWriters.containsKey(topic)) { openWriters(topic) } else { val w = createWriter(topic); openWriters.put(topic, w); w }
    
     if (writer.criticalError.get())
      throw new RuntimeException("Critical errors while writing to filesystem")

     writer
    }
  }

  def createWriter(topic: String) = {
    new LogFileWriter(topics(topic), compressionPoolFactory, statusActor)
  }

  def close() = {
    execSerivce.shutdownNow()
    for (writer <- openWriters.values){
      logger.info("Stopping writer: " + writer)
      writer !? 'stop
    }
  }

  /**
   * Service that is woken up with a Scheduled Fixed Delay to notify each LogFileWriter to check for files that need rolling.
   */
  class RollService extends Runnable {

    override def run() = {
      try {
        val writers =  openWriters.synchronized {openWriters.values().toArray(Array[LogFileWriter]())}
        
        for (writer <- writers)
          writer ! 'checkRolls

      } catch {
        case e => logger.error(e.toString(), e)
      }
    }

  }

  class StatusPrintService(statusActor:StatusActor) extends Runnable {
	val logger = Logger.getLogger(classOf[StatusPrintService])
	
    override def run() = {
      try {
        statusActor ! ('log, logger)
      } catch {
        case e => logger.error(e.toString(), e)
      }
    }

  }

}

trait StatusActorMBean {
  def topics():Array[String]
  def topicStatus(topic:String):Long
}

class StatusActor extends Actor with StatusActorMBean{
  
  jmxRegister(this, "JMXFileLogResource:name=LogConsumerStatus")
  val messageReceivedTSMap = collection.mutable.Map[String, Long]()
  
  start
  
  override def topics() = messageReceivedTSMap.keys.toArray
  override def topicStatus(topic:String) = messageReceivedTSMap(topic)
  
  override def exceptionHandler = {
    case e => e.printStackTrace()
  }
  
  def act() {
    loop {
      react {
        case ('log, logger:Logger) => 
          printDetails(logger)
        case topic:String =>
           messageReceivedTSMap(topic) = System.currentTimeMillis()
        case m: Any =>
          println("Coult no understand: " + m)
      }
    }
  }
  
  def printDetails(log:Logger) = {
    log.info("Message consume update : " + messageReceivedTSMap.size + " topics")
    val currTS = System.currentTimeMillis()
    for((topic, ts) <- messageReceivedTSMap){
      log.info(topic + " received messages " + (currTS - ts) + "ms ago")
    }
    
  }
  
}

/**
 * Actor that handles the reading and writing of files determined by date for a topic.<br/>
 * Each file for the topic is itself handled as an Actor wrapped by the FileObj instance.<br/>
 */
class LogFileWriter(topicConfig: TopicConfig, compressionPoolFactory: CompressionPoolFactory, statusActor:StatusActor=null) extends Actor {
  val logger = Logger.getLogger(classOf[LogFileWriter])

  val baseDir = topicConfig.baseDir
  //ensure that the directory is created
  //  baseDir.mkdirs()

  if (!(baseDir.exists() && baseDir.canWrite()))
    throw new RuntimeException(baseDir + " does not exist or is not writable")

  val topic = topicConfig.topic
  val compressionPool = compressionPoolFactory.get(topicConfig.codec)
  val extension = topicConfig.codec.getDefaultExtension()

  val openFiles = scala.collection.mutable.Map[String, FileObj]()

  start

  val criticalError = new AtomicBoolean(false)

  def act() {
    loop {
      react {
        case (date: String, msg: String) =>
          write(date, msg.getBytes())
        case (date: String, msg: Array[Byte]) =>
          write(date, msg)
        case 'stop =>
          closeAll()
          reply('stopped)
          exit('stop)
        case 'checkRolls =>
          checkFilesToRoll()
        case 'flush =>
          flushAll()
        case 'stopped =>
          ; //ignore, the file obj responded to the stop command
        case m: Any =>
          logger.error("Coult not understand: " + m)
      }
    }
  }

  override def exceptionHandler = {
    case e =>
      criticalError.set(true)
      logger.error(e.toString(), e)
  }

  def flushAll() = {
    for ((date, fileObj) <- openFiles) fileObj ! 'flush

  }
  var checkI = 0
  
  def checkFilesToRoll() = {
	  
    //usefull notification printing
    if(openFiles.size == 0){
        if(checkI % 100 == 0){
           logger.info("checkFilesToRoll for " + openFiles.size + " open files")
           checkI = 0
        }else{
         checkI = checkI + 1
        }
    }else
    	logger.info("checkFilesToRoll for " + openFiles.size + " open files")
    
    val rollCheck = topicConfig.rollCheck
    for ((date, fileObj) <- openFiles; if (rollCheck.shouldRoll(fileObj.lastModTime(), fileObj.size()))) {
      fileObj ! 'stop //stop and close
      openFiles -= date //remove this file from the open files list
    }
  }

  def write(date: String, msg: Array[Byte]) ={
    //tried with existOrUpdate but with mutable maps it seems sometimes fail,
    //using the imperative steps here works
    if(openFiles.containsKey(date)){
    	openFiles(date) wal msg
    }else{
      val file = createFile(date)
      openFiles.put(date, file)
      file wal msg
    }
  }

  def createFile(date: String) = {
    new FileObj(new File(baseDir, topic + "." + date + "." + System.currentTimeMillis() + extension + "_"), compressionPool, topicConfig, statusActor)
  }

  def closeAll() =
    for (fileObj <- openFiles.values) fileObj !? 'stop

}

object FileObjUtil{
  val nArray = Array('\n'.toByte)
}

/**
 * Handles the complete life cycle of creating a Compression OutputStream and closing releasing the stream from the CompressionPool.
 */
case class FileObj(file: File, compression: CompressionPool, topicConfig: TopicConfig, statusActor:StatusActor=null) extends Actor {

  val logger = Logger.getLogger(classOf[FileObj])
  
  val fileOut = new FileOutputStream(file)
  val output = compression.create(fileOut, 1000, TimeUnit.MILLISECONDS)
  var modTs = System.currentTimeMillis()

  val walLog = new WALLog(new File(WALLog.fileName(file.getAbsolutePath())))

  val usenewLine = topicConfig.usenewLine
  val useBase64 = topicConfig.useBase64

  val base64 = new Base64()

  start()

  def lastModTime() = modTs

  def wal(msg: Array[Byte]) = {
    walLog << msg
    this ! msg
  }

  def act() {
    loop {
      react {
        case msg: Array[Byte] =>
          <<(msg)
        case 'stop =>
          close()
          reply('stopped)
          exit('stop)
        case 'flush =>
          output.flush()
      }
    }
  }

  var lines = 1L
  var linesPos = 0L
  var lastUpdateTS = System.currentTimeMillis()
  
  def <<(msg: => Array[Byte]) = {

    if (useBase64)
      output.write(base64.encode(msg))
    else
      output.write(msg)

    if (usenewLine)
      output.write(FileObjUtil.nArray)
      
    if(lines % 100000 == 0){
      val linesDiff = lines - linesPos
      linesPos = lines
      val ts = System.currentTimeMillis()
      val timeDiff = ts - lastUpdateTS
      lastUpdateTS = ts
      
      logger.info("Have written " + linesDiff + " messages in " + timeDiff + "ms to " + file.getAbsolutePath())
      
    }
      
    lines = lines + 1
    
    if(statusActor != null)
    	statusActor ! topicConfig.topic
    
    modTs = System.currentTimeMillis()
  }

  def size() = file.length()

  /**
   * Close and rename the file from name_ to name
   */
  def close() = {
    compression.closeAndRelease(output)
    file.renameTo(new File(file.getParentFile(), file.getName().init))
    walLog.destroy()
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