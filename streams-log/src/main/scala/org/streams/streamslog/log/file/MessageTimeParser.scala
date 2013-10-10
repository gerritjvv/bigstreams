package org.streams.streamslog.log.file

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Extract the date string from the current message
 */
trait MessageTimeParser[T] {

  def dateString(msg: T): String
  def millis(msg: T): Long

}

/**
 * Use to current date time, ignoring the message and its contents.
 */
object NowMessageTimeParser extends MessageTimeParser[Any] {

  /**
   * Returns yyyy-MM-dd-HH
   */
  def dateString(msg: Any) = new SimpleDateFormat("yyyy-MM-dd-HH").format(new Date())
  /**
   * Returns System.currentTimeMillis()
   */
  def millis(msg: Any) = System.currentTimeMillis()

}




