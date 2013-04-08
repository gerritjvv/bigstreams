package org.streams.streamslog.log.file

import org.junit.runner.RunWith
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import javassist.expr.Instanceof

@RunWith(classOf[JUnitRunner])
class TestMessageMetaDataParser extends FlatSpec with ShouldMatchers {

  /**
   * """
   *
   * var i = new JavaImporter(
   * org.streams.streamslog.log.file, java.lang, scala
   * );
   *
   * function parse(msg) { with(i) { return new MessageMetaData(msg, ["topic"], False); } }
   * """, "js"
   */
  val scriptParser = ScriptParser("""
         (import '(org.streams.streamslog.log.file MessageMetaData MessageMetaDataParser))
         (defn parse [topic, msg]
           (MessageMetaData. msg (into-array String ["topic"]) true, (System/currentTimeMillis))
         )
    
      """, "clj")

  val scriptParserJS = ScriptParser("""
		  function parse(topic, msg){
           return new Packages.org.streams.streamslog.log.file.MessageMetaData(msg, new Array("topics"), true, 
            new Date().getTime())
		  }
      """, "js")

  "scriptParser" should "parse" in {

    import scala.collection.JavaConversions._

    assert(scriptParser.parse("test", Array(1, 2, 3, 4)).isInstanceOf[MessageMetaData])

  }

  "scriptParserJS" should "parse" in {

    import scala.collection.JavaConversions._

    assert(scriptParserJS.parse("test", Array(1, 2, 3, 4)).isInstanceOf[MessageMetaData])

  }

  "scriptParser" should "parse and equal to 1 2 3 4" in {

    import scala.collection.JavaConversions._

    val msg = scriptParser.parse("test", Array(1, 2, 3, 4))
    println("msg: " + msg.msg)
    assert(msg.msg(0) == 1 && msg.msg(1) == 2 && msg.msg(2) == 3 && msg.msg(3) == 4)

  }

  "scriptParserJS" should "parse performance test" in {

    import scala.collection.JavaConversions._

    var total = 0L
    val arr: Array[Byte] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    (0 to 10000).foreach({ i: Int =>
      val start = System.currentTimeMillis()
      val msg = scriptParser.parse("test", arr)
      total = total + (System.currentTimeMillis() - start)
    })
    println("JS Time: " + (total / 10000D) + "ms")
  }
  
  "scriptParser" should "parse performance test" in {

    import scala.collection.JavaConversions._

    var total = 0L
    val arr: Array[Byte] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    (0 to 10000).foreach({ i: Int =>
      val start = System.currentTimeMillis()
      val msg = scriptParser.parse("test", arr)
      total = total + (System.currentTimeMillis() - start)
    })
    println("Clojure Time: " + (total / 10000D) + "ms")
  }
  
  
}