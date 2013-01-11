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

  def check(baseDir: File) {
    logger.info("Starting replay check for directory: " + baseDir.getAbsolutePath())
    WalReplay.replay(baseDir, {

      (topic, date, msg) =>
        logger.warn("Replaying Log for: topic=" + topic + " date=" + date)
        //send synchronously with a 10 second timeout
        logResource.get(topic) !? (10000L, (date, msg))
    }, true)

    logger.info("Completed Replay Check")
  }

}