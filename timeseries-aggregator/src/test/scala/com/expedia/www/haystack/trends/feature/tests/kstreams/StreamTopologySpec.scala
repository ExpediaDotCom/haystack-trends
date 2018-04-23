package com.expedia.www.haystack.trends.feature.tests.kstreams

import com.expedia.www.haystack.commons.entities.encoders.PeriodReplacementEncoder
import com.expedia.www.haystack.commons.health.HealthController
import com.expedia.www.haystack.trends.config.ProjectConfiguration
import com.expedia.www.haystack.trends.config.entities.KafkaConfiguration
import com.expedia.www.haystack.trends.feature.FeatureSpec
import com.expedia.www.haystack.trends.kstream.StreamTopology
import org.easymock.EasyMock

import scala.collection.immutable.HashMap

class StreamTopologySpec extends FeatureSpec {

  feature("The stream topology should set the app status as unhealthy in case the environment is not setup correctly") {


    scenario("an invalid kafka configuration") {

      Given("an invalid kafka configuration")
      val kafkaConfig = KafkaConfiguration(null, null, null, null, null, 0l)
      val stateStoreConfigs = new HashMap[String, String]
      val projectConfiguration = mock[ProjectConfiguration]
      expecting {
        projectConfiguration.kafkaConfig.andReturn(kafkaConfig).times(2)
        projectConfiguration.stateStoreConfig.andReturn(stateStoreConfigs)
        projectConfiguration.encoder.andReturn(new PeriodReplacementEncoder)
        projectConfiguration.enableStateStoreLogging.andReturn(false)
      }
      EasyMock.replay(projectConfiguration)

      When("the stream topology is started")
      val topology = new StreamTopology(projectConfiguration)
      topology.start()

      Then("the app health should be set to unhealthy without throwing an exception")
      HealthController.isHealthy shouldBe false

      topology.close()
    }
  }
}
