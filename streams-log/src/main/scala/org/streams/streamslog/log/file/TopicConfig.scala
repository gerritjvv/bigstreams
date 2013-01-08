package org.streams.streamslog.log.file

import java.io.File

import scala.io.Source

import org.apache.hadoop.io.compress.CompressionCodec

/**
 * Configuration for a Topic to consume from kafka
 */
case class TopicConfig(topic:String, rollCheck:RollCheck, codec:CompressionCodec, baseDir:File)


object TopicConfigParser{
  
  
   def apply(file:File):Array[TopicConfig] = {
       Source.fromFile(file).getLines().map( apply(_) ).toArray
   }
   
   /**
    * Parse a line of format topic:TSParserClass,timeout,sizeInBytes,compressionCodecClass,basedir
    */
   def apply(line:String) = {
     
     val items = line.split(":")
     if(items.size != 5)
        throw new RuntimeException("TopicConfig Format must be topic:TSParser:timeout,sizeinBytes:compression:basedir")
     
     
     val topic = items(0).trim()
     val tsParser = items(1).trim() match{ 
       case n:String if n.toUpperCase() == "NOW" => NowMessageTimeParser
       case n:String => Thread.currentThread().getContextClassLoader().loadClass(n).newInstance().asInstanceOf[MessageTimeParser[Any]]
     }
     
     val rollOverCheckParams = items(2).split(',').map(_.toLong)
    
     if(rollOverCheckParams.size != 2)
        throw new RuntimeException("Rollover Check parameters must be [timeout],[sizeInBytes]")
     
     val compression = 
       Thread.currentThread().getContextClassLoader().loadClass(items(3).trim()).newInstance().asInstanceOf[CompressionCodec]
     
       
     new TopicConfig(topic, new DateSizeCheck(rollOverCheckParams(0), rollOverCheckParams(1)), compression, new File(items(4).trim()))
     
   }
   
}