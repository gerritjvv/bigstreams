package org.streams.streamslog.log.file

import java.io.File
import java.io.FileOutputStream
import java.io.RandomAccessFile
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import scala.actors.Actor
import org.streams.commons.compression.CompressionPool
import org.streams.commons.compression.CompressionPoolFactory
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl
import org.streams.commons.status.Status
import org.streams.commons.status.Status.STATUS
import java.nio.channels.FileChannel

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
    openFiles.getOrElseUpdate(date, createFile(date)) wal msg

  def createFile(date: String) =
    new FileObj(new File(baseDir, topic + "." + date + "." + System.currentTimeMillis() + extension + "_"), compressionPool)

  def closeAll() =
    for (fileObj <- openFiles.values) fileObj ! 'stop

}

/**
 * Handles the complete life cycle of creating a Compression OutputStream and closing releasing the stream from the CompressionPool.
 */
case class FileObj(file: File, compression: CompressionPool) extends Actor {

  val fileOut = new FileOutputStream(file)
  val output = compression.create(fileOut, 1000, TimeUnit.MILLISECONDS)
  var modTs = System.currentTimeMillis()

  val walLog = new WALLog(new File(file.getAbsolutePath() + "-wal"))
  
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
    walLog.destroy()
  }

}

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