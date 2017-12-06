package com.expedia.www.haystack.trends.feature.tests.app

import com.codahale.metrics.JmxReporter
import com.expedia.www.haystack.trends.{ShutdownHookThread, StreamTopology}
import com.expedia.www.haystack.trends.feature.FeatureSpec

class ShutdownHookThreadSpec extends FeatureSpec {

  feature("The Shutdown Thread when invoked with close all the resources") {


    scenario("mocked jmx reporter and stream topology") {

      Given("mocked jmx reporter and stream topology")

      val reporter = mock[JmxReporter]
      val topology = mock[StreamTopology]

      When("the shutdown hook is invoked")
      expecting {
        reporter.close()
        topology.close()
      }

      Then("it should close the reporter and the topology")
      whenExecuting(reporter, topology) {
        val shutdownHookThread = new ShutdownHookThread(topology, reporter)
        shutdownHookThread.run()
      }
    }
  }
}
