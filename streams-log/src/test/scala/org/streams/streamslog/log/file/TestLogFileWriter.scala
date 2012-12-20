package org.streams.streamslog.log.file

import java.io.File
import org.apache.hadoop.io.compress.GzipCodec
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.FileUtils

@RunWith(classOf[JUnitRunner])
class TestLogFileWriter extends FlatSpec with ShouldMatchers with CompressionSuite{

  
  val baseDir = new File("target/test/testLogFileWriter")
  baseDir.mkdirs()

  val topicConfig = new TopicConfig("test", new DateSizeCheck(100, 100), new GzipCodec(), baseDir)
  
  //class LogFileWriter(topicConfig:TopicConfig, compressionPoolFactory:CompressionPoolFactory) extends Actor {
  val logWriter = withFactory( { factory => new LogFileWriter(topicConfig, factory)})
  
  "LogWriter" should "writeTwoFiles" in {
    
    logWriter ! ("2012-12-01", "Hi".getBytes())
    logWriter ! ("2012-12-02", "Hi".getBytes())
    Thread.sleep(1000)
    
    
    
    listFiles(baseDir, _.contains("test.2012-12-01")).length should be > (0)
    listFiles(baseDir, _.contains("test.2012-12-02")).length should be > (0)
    
  }
  
  "LogWriter" should "roll Two Files" in {
    
    logWriter ! 'checkRolls
    Thread.sleep(1000)
    
    listFiles(baseDir, s => s.contains("test.2012-12-01") && s.endsWith("gz")).length should be > (0)
    listFiles(baseDir, s => s.contains("test.2012-12-02") && s.endsWith("gz")).length should be > (0)
    
  }
  
  "LogWriter" should "speedtest" in {
    println("Hi")
    
    val start = System.currentTimeMillis()
    for(i <- (0 until 10)){
      logWriter ! ("2012-12-01", "this is a value")
    }
    
    println("Time: " + (System.currentTimeMillis()-start))
    logWriter ! 'stop
    Thread.sleep(1000)
  }
  
  
}