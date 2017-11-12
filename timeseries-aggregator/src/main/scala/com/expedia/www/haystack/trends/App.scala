/*
 *
 *     Copyright 2017 Expedia, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */

package com.expedia.www.haystack.trends

import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.trends.commons.health.{HealthController, UpdateHealthStatusFile}
import com.expedia.www.haystack.trends.commons.metrics.MetricsSupport
import com.expedia.www.haystack.trends.config.ProjectConfiguration._
import com.expedia.www.haystack.trends.kstream.StreamTopology
import org.slf4j.LoggerFactory


object App extends MetricsSupport {
  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  private var topology: StreamTopology = _
  private var jmxReporter: JmxReporter = _

  def main(args: Array[String]): Unit = {
    HealthController.addListener(new UpdateHealthStatusFile(healthStatusFilePath))

    startJmxReporter()
    topology = new StreamTopology(kafkaConfig)
    topology.start()

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = shutdown()
    }))
  }

  private def startJmxReporter() = {
    jmxReporter = JmxReporter.forRegistry(metricRegistry).build()
    jmxReporter.start()
  }

  private def shutdown(): Unit = {
    LOGGER.info("Shutdown hook is invoked, tearing down the application.")
    if (topology != null) topology.close()
    if (jmxReporter != null) jmxReporter.close()
    shutdownLogger()
  }

  private def shutdownLogger(): Unit = {
    val factory = LoggerFactory.getILoggerFactory
    val clazz = factory.getClass
    try {
      clazz.getMethod("stop").invoke(factory) // logback
    } catch {
      case _: ReflectiveOperationException =>
        try {
          clazz.getMethod("close").invoke(factory) // log4j
        } catch {
          case _: Exception =>
        }
      case _: Exception =>
    }
  }


}

}
