package org.streams.streamslog.log.file

import org.apache.hadoop.io.compress.CompressionCodec
import java.io.File

case class TopicConfig(topic:String, rollCheck:RollCheck, codec:CompressionCodec, baseDir:File)
