package org.streams.streamslog.log.file

import java.io.File

import scala.io.Source

import org.apache.hadoop.io.compress.CompressionCodec

/**
 * Configuration for a Topic to consume from kafka
 */
case class TopicConfig(topic:String, tsParser:MessageTimeParser[Any], rollCheck:RollCheck, codec:CompressionCodec, baseDir:File, useBase64:Boolean=false, 
    usenewLine:Boolean=false, threads:Int=2)


object TopicConfigParser{
  
  
   def apply(file:File):Array[TopicConfig] = {
       Source.fromFile(file).getLines().map( apply(_) ).toArray
   }
   
   /**
    * Parse a line of format topic:TSParserClass:timeout,sizeInBytes,compressionCodecClass:basedir:base64,threads,useNewLine
    */
   def apply(line:String) = {
     
     val items = line.split(":")
     if( !( items.size == 5 || items.size == 8 ))
        throw new RuntimeException("TopicConfig Format must be topic:TSParser:timeout,sizeinBytes:compression:basedir:useBase64:threads:useNewLine")
     
     
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
     
     val (useBase64, threads, usenewLine) = if(items.size == 8){ (items(5).toBoolean, items(6).toInt, items(7).toBoolean) }else{ (false, 2, false)}
     
     new TopicConfig(topic, tsParser, new DateSizeCheck(rollOverCheckParams(0), rollOverCheckParams(1)), compression, new File(items(4).trim()), useBase64, usenewLine, threads)
     
   }
   
}