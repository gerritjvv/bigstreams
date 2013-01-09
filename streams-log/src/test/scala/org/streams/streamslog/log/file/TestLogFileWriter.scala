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

  val topicConfig = new TopicConfig("test", NowMessageTimeParser, new DateSizeCheck(100, 100), new GzipCodec(), baseDir, false, false, 1)
  
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
  
//  "LogWriter" should "speedtest" in {
//    
//    val start = System.currentTimeMillis()
//    
//    for(a <- (0 until 25)){
//    for( i <- (0 until 1000)){
//      val d = if(a > 9) a.toString() else "0" + a 
//      logWriter ! ("2012-12-"+ d, "this is a value\n")
//      
//    }
//    logWriter ! 'checkRolls
//    }
//    
//    println("Time: " + (System.currentTimeMillis()-start))
//    logWriter !? 'stop
//    println("Stopped in Time: " + (System.currentTimeMillis()-start))
//    
//  }
  
  
}