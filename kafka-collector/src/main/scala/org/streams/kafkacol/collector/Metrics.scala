package org.streams.kafkacol.collector

import java.util.concurrent.atomic.AtomicReference
import org.mortbay.jetty.Server
import org.mortbay.jetty.servlet.Context
import com.codahale.metrics.JmxReporter
import com.codahale.metrics.Metric
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.health.HealthCheck
import com.codahale.metrics.health.HealthCheckRegistry
import org.mortbay.jetty.servlet.ServletHolder
import com.codahale.metrics.servlets.HealthCheckServlet
import com.codahale.metrics.servlets.MetricsServlet
import com.codahale.metrics.servlets.PingServlet

object Metrics {

  val registry = new MetricRegistry()
  val healthRegistry = new HealthCheckRegistry()
  
  val reporter = JmxReporter.forRegistry(registry).build()
   reporter.start()  
  
  val serverRef = new AtomicReference[Server]();
  
  /**
   * We start jetty to monitoring health etc
   */
  def startHttp(port:Int) = {
	 
     if(serverRef.get() == null){
	    
	    val server1 = new Server(port)
	    val ctx = new Context(server1, "/metrics", Context.SESSIONS)
	    
	    ctx.setAttribute(MetricsServlet.METRICS_REGISTRY, registry)
	    ctx.setAttribute(HealthCheckServlet.HEALTH_CHECK_REGISTRY, healthRegistry)
	    
	    ctx.addServlet(new ServletHolder(
	                     new HealthCheckServlet()
	                   ), "/health")
	    ctx.addServlet(new ServletHolder(
	                     new MetricsServlet()
	                   ), "/metrics")
	    ctx.addServlet(new ServletHolder(
	                     new PingServlet()
	                   ), "/ping")
	                                
	                   
	    server1.start()
	    
	    serverRef.set(server1)
	 }
	  
	  
  }
   
  
  def register(name:String, check:HealthCheck) = {
    healthRegistry.register(name, check)
  }
  
  def register[T <: Metric](name:String, check:T) = {
	  registry.register(name, check)
  }
  
  def meter(name:String) = registry.meter(name)
  
  def histogram(name:String) = registry.histogram(name)
  
  
  def shutdown = {
    reporter.stop()
  }
  
}