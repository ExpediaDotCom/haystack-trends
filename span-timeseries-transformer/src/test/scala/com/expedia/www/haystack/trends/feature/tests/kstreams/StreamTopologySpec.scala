package com.expedia.www.haystack.trends.feature.tests.kstreams

import com.expedia.www.haystack.commons.health.HealthController
import com.expedia.www.haystack.trends.StreamTopology
import com.expedia.www.haystack.trends.config.entities.{KafkaConfiguration, TransformerConfiguration}
import com.expedia.www.haystack.trends.feature.FeatureSpec

class StreamTopologySpec extends FeatureSpec {

  feature("The stream topology should set the app status as unhealthy in case the environment is not setup correctly") {


    scenario("an invalid kafka configuration") {

      Given("an invalid kafka configuration")
      val kafkaConfig = KafkaConfiguration(null, null, null, null, null, 0l)
      val transformerConfig = TransformerConfiguration(true, true, List())

      When("the stream topology is started")
      val topology = new StreamTopology(kafkaConfig, transformerConfig)
      topology.start()

      Then("the app health should be set to unhealthy without throwing an exception")
      HealthController.isHealthy shouldBe false
      topology.close()
    }


  }
}
