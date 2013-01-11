package org.streams.streamslog.log.file

import java.io.File

import scala.collection.mutable.ArrayBuffer

import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class TestWALReplay extends FlatSpec with ShouldMatchers with CompressionSuite{

  
  def createWalLog(baseDir:File)= {
    
	  val topic = "test"
	  val date = "2013-01-01-01"
	  val walFile = new File(baseDir, topic + "." + date + ".myfile-wal")
	  walFile.createNewFile()
	  val walLog = new WALLog(walFile)
	 
	  val len = 10
	  
	  for( i <- 0 until len)
	    walLog << Array[Byte](1,2,3,i.asInstanceOf[Byte])
	  
	  walLog.close()
	 
	  (topic, date, walFile)
  }
  
  
  val baseDir = new File("target/test/testWALReplay")
  baseDir.mkdirs()

  

  
  "WALLog" should "replay and resume messages" in {
	
      val (topic, date, walFile) = createWalLog(baseDir)
      val walLogRepl = WALLog.replayWalLog(walFile)
      
      val buff = ArrayBuffer[Array[Byte]]()
	  walLogRepl.replay(   {(msg:Array[Byte]) =>
	      println("msg: " + java.util.Arrays.toString(msg))
          buff.append(msg)
      }, 2)
    
      buff.size should equal(2)
        
	  walLogRepl.replay(   {(msg:Array[Byte]) =>
	    println("msg: " + java.util.Arrays.toString(msg))
          buff.append(msg)
      }, 2)
    
      buff.size should equal(4)
      
      //read all
	  walLogRepl.replay(   {(msg:Array[Byte]) =>
	    println("msg: " + java.util.Arrays.toString(msg))
          buff.append(msg)
      })
    
      buff.size should equal(10)
  }
  
  "WalReplay" should "replay all messages" in {
	  val (topic, date, walFile) = createWalLog(baseDir)
      val walLogRepl = WALLog.replayWalLog(walFile)
      val buff = ArrayBuffer[Array[Byte]]()
      WalReplay.replayFile(walFile, true, 
          {
            (topic, date, msg) =>
              println("2:msg: " + java.util.Arrays.toString(msg))
              buff.append(msg)
          }
      )
      
      buff.size should equal(10)
  }
  
}