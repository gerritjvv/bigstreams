package org.streams.streamslog.log.file

import java.io.File
import javax.script.Invocable
import javax.script.ScriptEngineFactory
import javax.script.ScriptEngine
import javax.script.ScriptEngineManager
import com.googlecode.scalascriptengine.ScalaScriptEngine
import scala.io.Source

case class MessageMetaData(msg: Array[Byte], topics: Array[String], accept: Boolean = true, 
  ts:Long=System.currentTimeMillis())

/**
 * All interfaces
 */
trait MessageMetaDataParser {
  def parse(topic:String, msg: Array[Byte]): MessageMetaData
}

/**
 * Returns a MessageMetaData instance with the current time in millis
 */
object DefaultMessageMetaDataParser extends MessageMetaDataParser{
  
	def parse(topic:String, msg:Array[Byte]) = new MessageMetaData(msg, Array(topic))
}

object ScriptParser {
  import scala.collection.JavaConversions._

  val em = new ScriptEngineManager()
  var map = Map[String, ScriptEngine]()

  def findExtension(ext: String) = ext.slice(ext.lastIndexOf('.') + 1, ext.length())

  for (f <- em.getEngineFactories()) println("ScriptEngine: " + f.getExtensions())

  def getScriptEngine(ext: String): ScriptEngine =
    map.get(ext) match {
      case Some(value) => value
      case None =>
        val eng = em.getEngineByExtension(findExtension(ext))
        
        if (eng == null)
          throw new RuntimeException("Could not find engine for " + ext)
        
        println("Using Engine: " + eng.get(ScriptEngine.NAME) + " THREADING: " + eng.get("THREADING"))
        
        map = map + (ext -> eng)
        return eng
    }

   /**
   * Creates an instance of ScriptParser that implements the MessageMetaDataParser interface.
   */
  def apply(scriptFile: File):ScriptParser = 
    apply(Source.fromFile(scriptFile).mkString, findExtension(scriptFile.getName()))
  
  /**
   * Creates an instance of ScriptParser that implements the MessageMetaDataParser interface.
   */
  def apply(script: String, scriptExt: String):ScriptParser = {
    new ScriptParser(script,
      getScriptEngine(scriptExt))
  }

  /**
   * Creates a copy of the ScriptParse instantiating a new ScriptEngine instance
   */
  def apply(parser:ScriptParser):ScriptParser = {
      return new ScriptParser(parser.getScript, em.getEngineByName(parser.engineName))
  }
  
}

/**
 * Takes the script engine and prepares an Invocable instance after evaluting the script.<br/>
 * On each parse method call, the "parse" method of the script is called.<br/>
 */
 class ScriptParser(script: String, scriptEngine: ScriptEngine) extends MessageMetaDataParser {

  scriptEngine.eval(script)
  val parser: Invocable = scriptEngine.asInstanceOf[Invocable]

  if(parser == null)
    throw new RuntimeException("Unable to locate a function for the interface MessageMetaDataParser")
 
  def getScript = script
  def engineName = scriptEngine.get(ScriptEngine.NAME).toString
  def isThreadSafe = if( scriptEngine.get("THREADING") != null) true else false
  
  def parse(topic:String, msg: Array[Byte]): MessageMetaData = { 
    parser.invokeFunction("parse", topic, msg).asInstanceOf[MessageMetaData]
  }
  
}