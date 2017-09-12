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

package com.expedia.www.haystack.metricpoints

import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.metricpoints.config.ProjectConfiguration._
import com.expedia.www.haystack.metricpoints.health.{HealthController, UpdateHealthStatusFile}
import com.expedia.www.haystack.metricpoints.kstream.StreamTopology
import com.expedia.www.haystack.metricpoints.metrics.MetricsSupport


object App extends MetricsSupport {

  private var topology: StreamTopology = _
  private var jmxReporter: JmxReporter = _

  def main(args: Array[String]): Unit = {
    HealthController.addListener(new UpdateHealthStatusFile(healthStatusFilePath))

    startJmxReporter()
    topology = new StreamTopology(kafkaConfig)
    topology.start()

    Runtime.getRuntime.addShutdownHook(new ShutdownHookThread)
  }

  private def startJmxReporter() = {
    jmxReporter = JmxReporter.forRegistry(metricRegistry).build()
    jmxReporter.start()
  }

  private class ShutdownHookThread extends Thread {
    override def run(): Unit = {
      if(topology != null) topology.close()
      if(jmxReporter != null) jmxReporter.close()
    }
  }
}
