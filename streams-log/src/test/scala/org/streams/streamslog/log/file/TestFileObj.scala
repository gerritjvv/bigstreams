package org.streams.streamslog.log.file

import org.scalatest.FunSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FlatSpec
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl
import java.io.File
import org.apache.hadoop.io.compress.GzipCodec

@RunWith(classOf[JUnitRunner])
class TestFileObj extends FlatSpec with ShouldMatchers with CompressionSuite{

  val baseDir = new File("target/test/testFileObj")
  val file = new File(baseDir, "file.txt_")
  
  if(file.exists())
    file.delete();
  
  baseDir.mkdirs()
  file.createNewFile()
  
  val topicConfig = TopicConfigParser("test0:NOW:111,222:" + classOf[GzipCodec].getName() + ":target0")
  val fileObj = withCompressionPool({ pool => new FileObj(file, pool, topicConfig) })
  
  "FileObj" should "open" in {
     fileObj ! "Hi".getBytes()
     file.exists() should equal (true)
  }
  
  "FileObj" should "close and roll" in {
    fileObj ! 'stop
    Thread.sleep(1000) //should have stopped now
    
    new File(file.getAbsolutePath().init).exists() should equal (true)
    
  }
  
}