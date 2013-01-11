package org.streams.streamslog.log.file

import java.io.File

import scala.annotation.tailrec

import org.apache.log4j.Logger

/**
 * Lists all files in a directory and replay the logs
 */
object WalReplay {

  val logger = Logger.getLogger(getClass())

  /**
   * Reads through each files and send the message to the function f(topic, date, message)
   */
  def replay(dir: File, f: (String, String, Array[Byte]) => Unit, delete: Boolean) = {

    for (file <- dir.listFiles().withFilter(_.getName().endsWith("-wal")))
      replayFile(file, delete, f)

  }
  /**
   * Replay's a WAL file of format : base64(msg)\n
   * if delete == true then the -wal file and its companion file is deleted
   */
  def replayFile(file: File, delete: Boolean, f: (String, String, Array[Byte]) => Unit) = {
	  
    val (topic, date) = extractTopicDate(file)
    //replay the log file and send each message to the function f
	WALLog.replayWalLog(file, true, {msg:Array[Byte]} => f(topic, date, msg) )
	
  }

  
  def extractTopicDate(file: File) = {
    val split = file.getName().split('.')
    (split(0), split(1))
  }

}

