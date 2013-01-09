package org.streams.streamslog.log.file

import java.io.File
import java.io.FileOutputStream
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import scala.actors.Actor
import org.apache.commons.codec.binary.Base64
import org.streams.commons.compression.CompressionPool
import org.streams.commons.compression.CompressionPoolFactory
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl
import org.streams.commons.status.Status
import org.streams.commons.status.Status.STATUS
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.log4j.Logger

/**
 * Manages File Writers for topics, each FileWriter in turn manages its files based on date.<br/>
 * 
 */
class FileLogResource(topics: Map[String, TopicConfig], compressors:Int=100) {

  //check that all directories do exist
  for(topic <- topics.values){
    if(!topic.baseDir.exists()) topic.baseDir.mkdirs()
    
    if(!(topic.baseDir.exists() && topic.baseDir.canWrite()))
      throw new RuntimeException("The directory " + topic.baseDir.getAbsolutePath() + " does not exist or is not writable")
  }
  
  val openWriters = scala.collection.mutable.Map[String, LogFileWriter]()
  val compressionPoolFactory = new CompressionPoolFactoryImpl(compressors, compressors, NonStatus)

  val execSerivce = Executors.newSingleThreadScheduledExecutor()

  execSerivce.scheduleWithFixedDelay(new RollService(), 100L, 2000L, TimeUnit.MILLISECONDS)

  def get(topic: String) = {
    val writer = openWriters.getOrElseUpdate(topic, createWriter(topic))
    if(writer.criticalError.get())
       throw new RuntimeException("Critical errors while writing to filesystem") 
    
    writer
  }

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
  val logger = Logger.getLogger(classOf[LogFileWriter])
  
  val baseDir = topicConfig.baseDir
  //ensure that the directory is created
//  baseDir.mkdirs()
  
  if(!(baseDir.exists() && baseDir.canWrite()))
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

  override def exceptionHandler = {
      case e => 
        	criticalError.set(true)
        	logger.error(e.toString(), e)
  }
  
  def flushAll() = {
      for((date, fileObj) <- openFiles) fileObj ! 'flush
      
  }
  def checkFilesToRoll() = {
      val rollCheck = topicConfig.rollCheck
	  for((date, fileObj) <- openFiles; if(rollCheck.shouldRoll(fileObj.lastModTime(), fileObj.size()))){
	    fileObj ! 'stop //stop and close
	    openFiles -= date //remove this file from the open files list
	  }
  }

  
  def write(date: String, msg: Array[Byte]) =
    openFiles.getOrElseUpdate(date, createFile(date)) wal msg

  def createFile(date: String) =
    new FileObj(new File(baseDir, topic + "." + date + "." + System.currentTimeMillis() + extension + "_"), compressionPool, topicConfig)

  def closeAll() =
    for (fileObj <- openFiles.values) fileObj ! 'stop

}

/**
 * Handles the complete life cycle of creating a Compression OutputStream and closing releasing the stream from the CompressionPool.
 */
case class FileObj(file: File, compression: CompressionPool, topicConfig:TopicConfig) extends Actor {

  val fileOut = new FileOutputStream(file)
  val output = compression.create(fileOut, 1000, TimeUnit.MILLISECONDS)
  var modTs = System.currentTimeMillis()
  
  
  val walLog = new WALLog(new File(file.getAbsolutePath() + "-wal"))
  
  val usenewLine = topicConfig.usenewLine
  val nArray = Array('\n'.toByte)
  val useBase64 = topicConfig.useBase64
  
  val base64 = new Base64()
  
  start()

  def lastModTime() = modTs
  
  def wal(msg:Array[Byte]) = {
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

  def <<(msg: => Array[Byte]) = {
    if(useBase64)
       output.write(base64.encode(msg))
    else
    	output.write(msg)
    
    if(usenewLine)
      output.write(nArray)
    
      
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
class WALLog(walFile: File){
  
    
  walFile.createNewFile()
  val walOut = new RandomAccessFile(walFile, "rw")
  val walChannel = walOut.getChannel()
  val wBuf = walChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1073741824);

  def <<(msg: => Array[Byte]) = wBuf.put(msg);
  
  
  
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