package com.expedia.www.haystack.trends

import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.commons.logger.LoggerUtils
import com.expedia.www.haystack.trends.kstream.StreamTopology
import org.slf4j.LoggerFactory

private class ShutdownHookThread(topology: StreamTopology, jmxReporter: JmxReporter) extends Thread {

  import com.expedia.www.haystack.trends.ShutdownHookThread._

  override def run(): Unit = {
    LOGGER.info("Shutdown hook is invoked, tearing down the application.")

    if (topology != null) topology.close()
    if (jmxReporter != null) jmxReporter.close()
    LoggerUtils.shutdownLogger()
  }
}

object ShutdownHookThread {
  private val LOGGER = LoggerFactory.getLogger(this.getClass)
}
