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
import com.expedia.www.haystack.trends.integration.IntegrationTestSpec
import com.expedia.www.haystack.trends.kstream.StreamTopology
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.scalatest.Sequential

import scala.collection.JavaConverters._
import scala.concurrent.duration._

@Sequential
class CountTrendsSpec extends IntegrationTestSpec {

  private val MAX_METRICPOINTS = 62
  private val numberOfWatermarkedWindows = 1

  "TimeSeriesAggregatorTopology" should {

    "aggregate count type metricPoints from input topic based on rules" in {
      Given("a set of metricPoints with type metric and kafka specific configurations")
      val METRIC_NAME = "received-span"
      // CountMetric
      val expectedOneMinAggregatedPoints: Int = MAX_METRICPOINTS - numberOfWatermarkedWindows - 1
      // Why one less -> won't be generated for  last (MAX_METRICPOINTS * 60)th second metric point
      val expectedFiveMinAggregatedPoints: Int = (MAX_METRICPOINTS / 5) - numberOfWatermarkedWindows
      val expectedFifteenMinAggregatedPoints: Int = (MAX_METRICPOINTS / 15) - numberOfWatermarkedWindows
      val expectedOneHourAggregatedPoints: Int = (MAX_METRICPOINTS / 60) - numberOfWatermarkedWindows
      val expectedTotalAggregatedPoints: Int = expectedOneMinAggregatedPoints + expectedFiveMinAggregatedPoints + expectedFifteenMinAggregatedPoints + expectedOneHourAggregatedPoints-1

      When("metricPoints are produced in 'input' topic async, and kafka-streams topology is started")
      produceMetricPointsAsync(MAX_METRICPOINTS, 10.milli, METRIC_NAME, MAX_METRICPOINTS * 60)
      new StreamTopology(mockProjectConfig).start()

      Then("we should read all aggregated metricPoint from 'output' topic")
      val waitTimeMs = 15000
      val result: List[KeyValue[String, MetricPoint]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived[String, MetricPoint](RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, expectedTotalAggregatedPoints, waitTimeMs).asScala.toList
      validateAggregatedMetricPoints(result, expectedOneMinAggregatedPoints, expectedFiveMinAggregatedPoints, expectedFifteenMinAggregatedPoints, expectedOneHourAggregatedPoints)
    }
  }
}
