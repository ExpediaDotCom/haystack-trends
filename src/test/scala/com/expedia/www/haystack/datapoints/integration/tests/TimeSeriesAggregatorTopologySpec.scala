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
package com.expedia.www.haystack.datapoints.integration.tests

import java.util.{List => JList}

import com.expedia.www.haystack.datapoints.config.entities.KafkaConfiguration
import com.expedia.www.haystack.datapoints.entities.DataPoint
import com.expedia.www.haystack.datapoints.integration.IntegrationTestSpec
import com.expedia.www.haystack.datapoints.kstream.StreamTopology
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.{KeyValue, StreamsConfig}

import scala.concurrent.duration._

class TimeSeriesAggregatorTopologySpec extends IntegrationTestSpec {

  private val MAX_DATAPOINTS = 5
  APP_ID = "haystack-topology-test"
  private val metricName = "duration"


  "TimeSeries Aggregator Topology" should {
    "consume datapoints from input topic and aggregate them based on rules" in {
      Given("a set of datapoints with type metric and kafka specific configurations")
      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG), OUTPUT_TOPIC, INPUT_TOPIC, AutoOffsetReset.EARLIEST, new WallclockTimestampExtractor)

      When("datapoints are produced in 'input' topic async, and kafka-streams topology is started")
      produceDataPointsAsync(MAX_DATAPOINTS, 1000.milli, metricName)
      new StreamTopology(kafkaConfig).start()

      Then("we should read one aggregated datapoint from 'output' topic")
      val result: JList[KeyValue[String, DataPoint]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 5, 15000)
      validateAggregatedDataPoints(result)
    }
  }

  private def validateAggregatedDataPoints(result: JList[KeyValue[String, DataPoint]]) = {
    result.get(0).value.metric shouldEqual metricName
  }


}

