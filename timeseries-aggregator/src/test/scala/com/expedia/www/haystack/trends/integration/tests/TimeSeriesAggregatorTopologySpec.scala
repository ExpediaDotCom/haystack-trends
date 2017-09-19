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
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.{KeyValue, StreamsConfig}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class TimeSeriesAggregatorTopologySpec extends IntegrationTestSpec {

  private val MAX_METRICPOINTS = 5
  APP_ID = "haystack-topology-test"
  private val metricName = "duration"


  "TimeSeries Aggregator Topology" should {
    "consume metricPoints from input topic and aggregate them based on rules" in {
      Given("a set of metricPoints with type metric and kafka specific configurations")
      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG), OUTPUT_TOPIC, INPUT_TOPIC, AutoOffsetReset.EARLIEST, new WallclockTimestampExtractor,30000)

      When("metricPoints are produced in 'input' topic async, and kafka-streams topology is started")
      produceMetricPointsAsync(MAX_METRICPOINTS, 10.milli, metricName, 100)
      new StreamTopology(kafkaConfig).start()

      Then("we should read one aggregated metricPoint from 'output' topic")
      val result: List[KeyValue[String, MetricPoint]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived[String, MetricPoint](RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 1, 15000).asScala.toList
      validateAggregatedMetricPoints(result)
    }
  }

  private def validateAggregatedMetricPoints(producedRecords: List[KeyValue[String, MetricPoint]]) = {

    producedRecords.foreach(record => {
      record.value.metric shouldEqual metricName
    })
  }

}

