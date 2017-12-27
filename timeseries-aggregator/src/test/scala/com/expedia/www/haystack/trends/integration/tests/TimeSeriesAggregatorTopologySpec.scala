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


import com.expedia.www.haystack.trends.commons.entities.{Interval, MetricPoint}
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

  private val MAX_METRICPOINTS = 62

  "TimeSeries Aggregator Topology" should {
    "consume metricPoints from input topic and aggregate them based on rules for histogram type" in {
      Given("a set of metricPoints with type metric and kafka specific configurations")
      val METRIC_NAME = "duration"  //HistogramMetric
      val expectedOneMinAggregatedPoints: Int = (MAX_METRICPOINTS - 1) * 7  // Why one less -> won't be generated for  last (MAX_METRICPOINTS * 60)th second metric point
      val expectedFiveMinAggregatedPoints: Int = (MAX_METRICPOINTS / 5) * 7
      val expectedFifteenMinAggregatedPoints: Int = (MAX_METRICPOINTS / 15) * 7
      val expectedOneHourAggregatedPoints: Int = (MAX_METRICPOINTS / 60) * 7
      val expectedTotalAggregatedPoints: Int = expectedOneMinAggregatedPoints + expectedFiveMinAggregatedPoints + expectedFifteenMinAggregatedPoints + expectedOneHourAggregatedPoints

      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG), OUTPUT_TOPIC, INPUT_TOPIC, AutoOffsetReset.EARLIEST, new WallclockTimestampExtractor, 30000)

      When("metricPoints are produced in 'input' topic async, and kafka-streams topology is started")
      produceMetricPointsAsync(MAX_METRICPOINTS, 10.milli, METRIC_NAME, MAX_METRICPOINTS * 60)
      new StreamTopology(kafkaConfig, true).start()

      Then("we should read all aggregated metricPoint from 'output' topic")
      val waitTimeMs = 15000
      val result: List[KeyValue[String, MetricPoint]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived[String, MetricPoint](RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, expectedTotalAggregatedPoints, waitTimeMs).asScala.toList
      validateAggregatedMetricPoints(result, expectedOneMinAggregatedPoints, expectedFiveMinAggregatedPoints, expectedFifteenMinAggregatedPoints, expectedOneHourAggregatedPoints)
    }
  }

    "consume metricPoints from input topic and aggregate them based on rules for Count type" in {
      Given("a set of metricPoints with type metric and kafka specific configurations")
      val METRIC_NAME = "received-span" // CountMetric
      val expectedOneMinAggregatedPoints: Int = MAX_METRICPOINTS - 1  // Why one less -> won't be generated for  last (MAX_METRICPOINTS * 60)th second metric point
      val expectedFiveMinAggregatedPoints: Int = MAX_METRICPOINTS / 5
      val expectedFifteenMinAggregatedPoints: Int = MAX_METRICPOINTS / 15
      val expectedOneHourAggregatedPoints: Int = MAX_METRICPOINTS / 60
      val expectedTotalAggregatedPoints: Int = expectedOneMinAggregatedPoints + expectedFiveMinAggregatedPoints + expectedFifteenMinAggregatedPoints + expectedOneHourAggregatedPoints

      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG), OUTPUT_TOPIC, INPUT_TOPIC, AutoOffsetReset.EARLIEST, new WallclockTimestampExtractor, 30000)

      When("metricPoints are produced in 'input' topic async, and kafka-streams topology is started")
      produceMetricPointsAsync(MAX_METRICPOINTS, 10.milli, METRIC_NAME, MAX_METRICPOINTS * 60)
      new StreamTopology(kafkaConfig, true).start()

      Then("we should read all aggregated metricPoint from 'output' topic")
      val waitTimeMs = 15000
      val result: List[KeyValue[String, MetricPoint]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived[String, MetricPoint](RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, expectedTotalAggregatedPoints, waitTimeMs).asScala.toList
      validateAggregatedMetricPoints(result, expectedOneMinAggregatedPoints, expectedFiveMinAggregatedPoints, expectedFifteenMinAggregatedPoints, expectedOneHourAggregatedPoints)
    }

  private def validateAggregatedMetricPoints(producedRecords: List[KeyValue[String, MetricPoint]],
                                             expectedOneMinAggregatedPoints: Int,
                                             expectedFiveMinAggregatedPoints: Int,
                                             expectedFifteenMinAggregatedPoints: Int,
                                             expectedOneHourAggregatedPoints: Int) = {

    val oneMinAggMetricPoints = producedRecords.filter(_.value.tags("interval").equals(Interval.ONE_MINUTE.toString()))
    val fiveMinAggMetricPoints = producedRecords.filter(_.value.tags("interval").equals(Interval.FIVE_MINUTE.toString()))
    val fifteenMinAggMetricPoints = producedRecords.filter(_.value.tags("interval").equals(Interval.FIFTEEN_MINUTE.toString()))
    val oneHourAggMetricPoints = producedRecords.filter(_.value.tags("interval").equals(Interval.ONE_HOUR.toString()))

    oneMinAggMetricPoints.size shouldEqual expectedOneMinAggregatedPoints
    fiveMinAggMetricPoints.size shouldEqual expectedFiveMinAggregatedPoints
    fifteenMinAggMetricPoints.size shouldEqual expectedFifteenMinAggregatedPoints
    oneHourAggMetricPoints.size shouldEqual expectedOneHourAggregatedPoints
  }
}

