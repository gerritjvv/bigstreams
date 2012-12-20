package org.streams.streamslog.log.file

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
  
  
  override def shouldRoll(lastUpdated:Long, fileSize:Long):Boolean = 
      (System.currentTimeMillis() - lastUpdated >= timeSinceUpdate) || scala.math.abs(fileSize-maxSize) < 500
  
}