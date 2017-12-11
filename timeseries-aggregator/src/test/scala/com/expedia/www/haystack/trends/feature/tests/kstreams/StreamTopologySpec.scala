package com.expedia.www.haystack.trends.feature.tests.kstreams

import com.expedia.www.haystack.trends.commons.health.HealthController
import com.expedia.www.haystack.trends.config.entities.KafkaConfiguration
import com.expedia.www.haystack.trends.feature.FeatureSpec
import com.expedia.www.haystack.trends.kstream.StreamTopology

class StreamTopologySpec extends FeatureSpec {

  feature("The stream topology should set the app status as unhealthy in case the environment is not setup correctly") {


    scenario("an invalid kafka configuration") {

      Given("an invalid kafka configuration")
      val kafkaConfig = KafkaConfiguration(null, null, null, null, null, 0l)

      When("the stream topology is started")
      val topology = new StreamTopology(kafkaConfig, true)
      topology.start()

      Then("the app health should be set to unhealthy without throwing an exception")
      HealthController.isHealthy shouldBe false
    }


  }
}
