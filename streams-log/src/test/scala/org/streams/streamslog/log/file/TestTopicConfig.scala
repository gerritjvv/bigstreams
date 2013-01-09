package org.streams.streamslog.log.file

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.hadoop.io.compress.GzipCodec
import java.io.File
import java.io.PrintWriter

@RunWith(classOf[JUnitRunner])
class TestTopicConfig extends FlatSpec with ShouldMatchers{

  "TopicConfigParser" should "parseFile" in{
    
    val dir = new File("target/test/testtopicConfig")
    dir.mkdirs()
    val file = new File(dir, "testconfig")
    file.createNewFile()
    
    val writer = new PrintWriter(file)
    //write comment #
    writer.println("#test0:NOW:111,222:" + classOf[GzipCodec].getName() + ":target0")
    //write comment //
    writer.println("//test0:NOW:111,222:" + classOf[GzipCodec].getName() + ":target0")
    writer.println("test0:NOW:111,222:" + classOf[GzipCodec].getName() + ":target0")
    writer.println("test1:NOW:111,222:" + classOf[GzipCodec].getName() + ":target1")
    writer.println("test2:NOW:111,222:" + classOf[GzipCodec].getName() + ":target2")
    writer.println("test3:NOW:111,222:" + classOf[GzipCodec].getName() + ":target3")
    writer.close()
    
    val configs = TopicConfigParser(file)
    
    configs.size should equal (4)
    
    configs.foldLeft(0) { (i:Int, config:TopicConfig) => assertConfig(config, i); i+1 }
    
    def assertConfig(config:TopicConfig, i:Int){
    	config.topic should equal("test"+i)
    	assert(config.codec.isInstanceOf[GzipCodec])
    	config.rollCheck == NowMessageTimeParser
    	config.baseDir.getName() should equal ("target"+i)
    }
    
  }
  
  "TopicConfigParser" should "parseLine" in {
    
	 val config = TopicConfigParser("test:NOW:111,222:" + classOf[GzipCodec].getName() + ":target")
    
	 config.topic should equal("test")
	 assert(config.codec.isInstanceOf[GzipCodec])
	 config.rollCheck == NowMessageTimeParser
	 config.baseDir.getName() should equal ("target")
  }
  
  
  
}