package org.streams.streamslog.log.file

import java.io.File
import java.io.FileOutputStream
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import scala.actors.Actor
import org.apache.commons.codec.binary.Base64
import org.apache.log4j.Logger
import org.streams.commons.compression.CompressionPool
import org.streams.commons.compression.CompressionPoolFactory
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl
import org.streams.commons.status.Status
import org.streams.commons.status.Status.STATUS
import org.streams.streamslog.jmx.JMXHelpers._
import org.streams.streamslog.log.file.StatusActor
import scala.reflect.BeanProperty
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

  val openWriters = scala.collection.mutable.Map[String, LogFileWriter]()
  val compressionPoolFactory = new CompressionPoolFactoryImpl(compressors, compressors, NonStatus)

  val execSerivce = Executors.newScheduledThreadPool(2)
  
  execSerivce.scheduleWithFixedDelay(new RollService(), 10L, 2L, TimeUnit.SECONDS)
  execSerivce.scheduleWithFixedDelay(new StatusPrintService(statusActor), 10L, 10L, TimeUnit.SECONDS)

  def get(topic: String) = {
    val writer = openWriters.getOrElseUpdate(topic, createWriter(topic))
    if (writer.criticalError.get())
      throw new RuntimeException("Critical errors while writing to filesystem")

    writer
  }

  def createWriter(topic: String) = {
    new LogFileWriter(topics(topic), compressionPoolFactory, statusActor)
  }

  def close() = {
    execSerivce.shutdownNow()
    for (writer <- openWriters.values) writer ! 'stop
  }

  /**
   * Service that is woken up with a Scheduled Fixed Delay to notify each LogFileWriter to check for files that need rolling.
   */
  class RollService extends Runnable {

    override def run() = {
      try {
        
        for (writer <- openWriters.values)
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
          exit('stop)
        case 'checkRolls =>
          checkFilesToRoll()
        case 'flush =>
          flushAll()
        case m: Any =>
          println("Coult no understand: " + m)
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

  def write(date: String, msg: Array[Byte]) =
    openFiles.getOrElseUpdate(date, createFile(date)) wal msg

  def createFile(date: String) =
    new FileObj(new File(baseDir, topic + "." + date + "." + System.currentTimeMillis() + extension + "_"), compressionPool, topicConfig, statusActor)

  def closeAll() =
    for (fileObj <- openFiles.values) fileObj ! 'stop

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

  val walLog = new WALLog(new File(file.getAbsolutePath() + "-wal"))

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
          exit('stop)
        case 'flush =>
          output.flush()
      }
    }
  }

  var lines = 1L
  
  def <<(msg: => Array[Byte]) = {

    if (useBase64)
      output.write(base64.encode(msg))
    else
      output.write(msg)

    if (usenewLine)
      output.write(FileObjUtil.nArray)
      
    if(lines % 10000 == 0)
      logger.info("Have written " + lines + " messages to " + file.getAbsolutePath())
      
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

/**
 * Use a Preallocated buffer to write out data using a BufferMap.<br/>
 * This WAL will not flush to disk thus may loose data, but data is kept in the OS RAM, so
 * in case of a crash most of the data will not be lost. This is a performance choice.
 */
class WALLog(walFile: File) {

  walFile.createNewFile()
  val walOut = new RandomAccessFile(walFile, "rw")
  val walChannel = walOut.getChannel()
  val wBuf = walChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1073741824);

  val base64 = new Base64()
  
  def <<(msg: => Array[Byte]) = {
    wBuf.put(base64.encode(msg));
    wBuf.put(FileObjUtil.nArray)
  }

  def destroy() = {
    walChannel.close()
    walOut.close();
    walFile.delete()
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