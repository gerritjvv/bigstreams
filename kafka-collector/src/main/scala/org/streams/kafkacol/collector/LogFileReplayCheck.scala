package org.streams.kafkacol.collector

import java.io.File
import org.streams.streamslog.log.file.FileLogResource
import org.streams.streamslog.log.file.WalReplay
import org.apache.log4j.Logger

/**
 * Check for WAL files from a previous shutdown and replay them.
 */
class LogFileReplayCheck(logResource: FileLogResource) {

  val logger = Logger.getLogger(classOf[LogFileReplayCheck])
  var i = 0
  
  def check(baseDir: File) {
    logger.info("Starting replay check for directory: " + baseDir.getAbsolutePath())
    WalReplay.replay(baseDir, {

      (topic, date, msg) =>
        if(i % 100000 == 0)
        	logger.warn("Replaying Log for: topic=" + topic + " date=" + date + " records replayed: " + i)
        i = i + 1
        //send synchronously with a 10 second timeout
        logResource.get(topic) ! (date, msg)
    }, true)
    
    logger.info("Completed Replay Check")
  }

}