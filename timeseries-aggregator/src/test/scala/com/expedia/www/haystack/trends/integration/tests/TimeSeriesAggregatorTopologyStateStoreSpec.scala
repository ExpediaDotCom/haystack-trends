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
package com.expedia.www.haystack.trends.integration.tests

import com.expedia.www.haystack.trends.commons.entities.MetricPoint
import com.expedia.www.haystack.trends.config.entities.KafkaConfiguration
import com.expedia.www.haystack.trends.integration.IntegrationTestSpec
import com.expedia.www.haystack.trends.kstream.StreamTopology
import org.apache.kafka.clients.admin.{AdminClient, Config}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.StreamsConfig

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class TimeSeriesAggregatorTopologyStateStoreSpec extends IntegrationTestSpec {

  private val WAIT_TIME_MS = 15000

  "State store" should {
    "have the configuration set by the topology" in {
      Given("a set of metricPoints with type metric and state store specific configurations")
      val METRIC_NAME = "received-span" // CountMetric
      val stateStoreConfigs = Map("cleanup.policy" -> "compact,delete")
      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG), OUTPUT_TOPIC, INPUT_TOPIC, AutoOffsetReset.EARLIEST, new WallclockTimestampExtractor, 30000)

      When("metricPoints are produced in 'input' topic async, and kafka-streams topology is started")
      produceMetricPointsAsync(3, 10.milli, METRIC_NAME, 120)
      new StreamTopology(kafkaConfig, stateStoreConfigs, true).start()

      Then("we should see the state store topic created with specified properties")
      IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived[String, MetricPoint](RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 1, WAIT_TIME_MS).asScala.toList
      val adminClient = AdminClient.create(STREAMS_CONFIG)
      val configResource = new ConfigResource(ConfigResource.Type.TOPIC, "haystack-trends-windowed-metric-store-changelog")
      val describeConfigResult: java.util.Map[ConfigResource, Config] = adminClient.describeConfigs(java.util.Arrays.asList(configResource)).all().get()
      describeConfigResult.get(configResource).get(stateStoreConfigs.head._1).value() shouldBe (stateStoreConfigs.head._2)
    }
  }
}

