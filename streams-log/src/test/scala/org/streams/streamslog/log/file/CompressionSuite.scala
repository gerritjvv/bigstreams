package org.streams.streamslog.log.file

import org.streams.commons.compression.CompressionPool
import org.streams.commons.compression.impl.CompressionPoolFactoryImpl
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.GzipCodec
import org.streams.commons.compression.CompressionPoolFactory
import java.io.File
import java.io.FilenameFilter

trait CompressionSuite {


  val poolFactory = new CompressionPoolFactoryImpl(10,10, NonStatus)
  
  def withCompressionPool[T]( f: CompressionPool => T ) ={
    
      f(poolFactory.get(new GzipCodec()))
      
  }
  
  
  def withFactory[T]( f: CompressionPoolFactory => T ) ={
    
      f(poolFactory)
      
  }
 
  
  def listFiles(dir:File,  f:(String) => Boolean) = 
      dir.list(new FilenameFilter() { override def accept(dir:File, name:String) = { f(name) }  } )
}