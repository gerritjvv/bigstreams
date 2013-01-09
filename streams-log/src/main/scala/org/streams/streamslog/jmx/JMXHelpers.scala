package org.streams.streamslog.jmx

import java.lang.management.ManagementFactory
import javax.management.ObjectName

/**
 * Shamelessly copied from http://www.crosson.org/2011/08/playing-with-jmx-and-scala-mbean.html
 */
object JMXHelpers {

  implicit def string2objectName(name:String):ObjectName = new ObjectName(name)
    def jmxRegister(ob:Object, obname:ObjectName) =
      ManagementFactory.getPlatformMBeanServer.registerMBean(ob, obname)
  
}