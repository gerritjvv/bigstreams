package org.streams.streamslog.log.file

import org.apache.log4j.Logger

/**
 * That defines if a file should roll
 */
trait RollCheck {

  def shouldRoll(lastUpdated:Long, fileSize:Long):Boolean
  
}


/**
 * Returns true if the fileSize is within 500 bytes of the file size, or if the time since last update has exceeded an certain amount of millis
 */
class DateSizeCheck(timeSinceUpdate:Long, maxSize:Long) extends RollCheck{
  
  val logger = Logger.getLogger(classOf[DateSizeCheck])
  
  override def shouldRoll(lastUpdated:Long, fileSize:Long):Boolean = {
      val tsdiff = System.currentTimeMillis() - lastUpdated 
      logger.info("DateSizeCheck: " + tsdiff + " >= " + timeSinceUpdate + " fileSize: " + fileSize + " >= ( " + (maxSize - 2048) + ")")
      (tsdiff >= timeSinceUpdate) || fileSize >= (maxSize - 2048)
      
  }
  
}