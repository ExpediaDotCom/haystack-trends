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
import com.expedia.www.haystack.trends.config.ProjectConfiguration
import org.slf4j.LoggerFactory

object App extends MetricsSupport {

  private var topology: StreamTopology = _
  private var jmxReporter: JmxReporter = _
  private val LOGGER = LoggerFactory.getLogger(this.getClass)
  private val projectConfiguration = new ProjectConfiguration()

  def main(args: Array[String]): Unit = {
    HealthController.addListener(new UpdateHealthStatusFile(projectConfiguration.healthStatusFilePath))

    startJmxReporter()
    topology = new StreamTopology(projectConfiguration.kafkaConfig,
      projectConfiguration.transformerConfiguration)
    topology.start()

    Runtime.getRuntime.addShutdownHook(new ShutdownHookThread(topology,jmxReporter))
  }

  private def startJmxReporter() = {
    jmxReporter = JmxReporter.forRegistry(metricRegistry).build()
    jmxReporter.start()
  }
}




