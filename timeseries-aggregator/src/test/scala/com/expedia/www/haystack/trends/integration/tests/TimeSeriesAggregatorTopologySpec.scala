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
import com.expedia.www.haystack.trends.config.ProjectConfiguration
import com.expedia.www.haystack.trends.config.entities.KafkaConfiguration
import com.expedia.www.haystack.trends.integration.IntegrationTestSpec
import com.expedia.www.haystack.trends.kstream.StreamTopology
import org.apache.kafka.clients.admin.{AdminClient, Config}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.{KeyValue, StreamsConfig}
import org.easymock.EasyMock
import org.scalatest.Sequential

import scala.collection.JavaConverters._
import scala.concurrent.duration._

@Sequential
class TimeSeriesAggregatorTopologySpec extends IntegrationTestSpec {

  private val MAX_METRICPOINTS = 62
  private val numberOfWatermarkedWindows = 1
  val stateStoreConfigs = Map("cleanup.policy" -> "compact,delete")

  "TimeSeriesAggregatorTopology" should {

    "watermark metrics for aggregate count type metricPoints from input topic" in {
      Given("a set of metricPoints with type metric and kafka specific configurations")
      val METRIC_NAME = "received-span" // CountMetric
      val expectedOneMinAggregatedPoints: Int = 3 // Why one less -> won't be generated for  last (MAX_METRICPOINTS * 60)th second metric point
      val expectedFiveMinAggregatedPoints: Int = 1
      val expectedFifteenMinAggregatedPoints: Int = 0
      val expectedOneHourAggregatedPoints: Int = 0
      val expectedTotalAggregatedPoints: Int = expectedOneMinAggregatedPoints + expectedFiveMinAggregatedPoints + expectedFifteenMinAggregatedPoints + expectedOneHourAggregatedPoints


      When("metricPoints are produced in 'input' topic async, and kafka-streams topology is started")
      produceMetricPoint(METRIC_NAME, 1l, 1l)
      produceMetricPoint(METRIC_NAME, 65l, 2l)
      produceMetricPoint(METRIC_NAME, 2l, 3l)
      produceMetricPoint(METRIC_NAME, 130l, 4l)
      produceMetricPoint(METRIC_NAME, 310l, 5l)
      produceMetricPoint(METRIC_NAME, 610l, 6l)

      new StreamTopology(mockProjectConfig).start()

      Then("we should read all aggregated metricPoint from 'output' topic")
      val waitTimeMs = 15000
      val result: List[KeyValue[String, MetricPoint]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived[String, MetricPoint](RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, expectedTotalAggregatedPoints, waitTimeMs).asScala.toList
      validateAggregatedMetricPoints(result, expectedOneMinAggregatedPoints, expectedFiveMinAggregatedPoints, expectedFifteenMinAggregatedPoints, expectedOneHourAggregatedPoints)
    }

    "aggregate count type metricPoints from input topic based on rules" in {
      Given("a set of metricPoints with type metric and kafka specific configurations")
      val METRIC_NAME = "received-span" // CountMetric
      val expectedOneMinAggregatedPoints: Int = MAX_METRICPOINTS - numberOfWatermarkedWindows - 1 // Why one less -> won't be generated for  last (MAX_METRICPOINTS * 60)th second metric point
      val expectedFiveMinAggregatedPoints: Int = (MAX_METRICPOINTS / 5) - numberOfWatermarkedWindows
      val expectedFifteenMinAggregatedPoints: Int = (MAX_METRICPOINTS / 15) - numberOfWatermarkedWindows
      val expectedOneHourAggregatedPoints: Int = (MAX_METRICPOINTS / 60) - numberOfWatermarkedWindows
      val expectedTotalAggregatedPoints: Int = expectedOneMinAggregatedPoints + expectedFiveMinAggregatedPoints + expectedFifteenMinAggregatedPoints + expectedOneHourAggregatedPoints

      When("metricPoints are produced in 'input' topic async, and kafka-streams topology is started")
      produceMetricPointsAsync(MAX_METRICPOINTS, 10.milli, METRIC_NAME, MAX_METRICPOINTS * 60)
      new StreamTopology(mockProjectConfig).start()

      Then("we should read all aggregated metricPoint from 'output' topic")
      val waitTimeMs = 15000
      val result: List[KeyValue[String, MetricPoint]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived[String, MetricPoint](RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, expectedTotalAggregatedPoints, waitTimeMs).asScala.toList
      validateAggregatedMetricPoints(result, expectedOneMinAggregatedPoints, expectedFiveMinAggregatedPoints, expectedFifteenMinAggregatedPoints, expectedOneHourAggregatedPoints)
    }

    "have state store (change log) configuration be set by the topology" in {
      Given("a set of metricPoints with type metric and state store specific configurations")
      val METRIC_NAME = "received-span" // CountMetric

      When("metricPoints are produced in 'input' topic async, and kafka-streams topology is started")
      produceMetricPointsAsync(3, 10.milli, METRIC_NAME, 3 * 60)
      new StreamTopology(mockProjectConfig).start()

      Then("we should see the state store topic created with specified properties")
      val waitTimeMs = 15000
      IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived[String, MetricPoint](RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 1, waitTimeMs).asScala.toList
      val adminClient = AdminClient.create(STREAMS_CONFIG)
      val configResource = new ConfigResource(ConfigResource.Type.TOPIC, CHANGELOG_TOPIC)
      val describeConfigResult: java.util.Map[ConfigResource, Config] = adminClient.describeConfigs(java.util.Arrays.asList(configResource)).all().get()
      describeConfigResult.get(configResource).get(stateStoreConfigs.head._1).value() shouldBe stateStoreConfigs.head._2
    }

    "aggregate histogram type metricPoints from input topic based on rules" in {
      Given("a set of metricPoints with type metric and kafka specific configurations")
      val METRIC_NAME = "duration" //HistogramMetric
      val expectedOneMinAggregatedPoints: Int = (MAX_METRICPOINTS - 1 - numberOfWatermarkedWindows) * 7 // Why one less -> won't be generated for  last (MAX_METRICPOINTS * 60)th second metric point
      val expectedFiveMinAggregatedPoints: Int = (MAX_METRICPOINTS / 5 - numberOfWatermarkedWindows) * 7
      val expectedFifteenMinAggregatedPoints: Int = (MAX_METRICPOINTS / 15 - numberOfWatermarkedWindows) * 7
      val expectedOneHourAggregatedPoints: Int = (MAX_METRICPOINTS / 60 - numberOfWatermarkedWindows) * 7
      val expectedTotalAggregatedPoints: Int = expectedOneMinAggregatedPoints + expectedFiveMinAggregatedPoints + expectedFifteenMinAggregatedPoints + expectedOneHourAggregatedPoints

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

  private def mockProjectConfig: ProjectConfiguration = {
    val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG), OUTPUT_TOPIC, INPUT_TOPIC, AutoOffsetReset.EARLIEST, new WallclockTimestampExtractor, 30000)
    val projectConfiguration = mock[ProjectConfiguration]

    expecting {
      projectConfiguration.kafkaConfig.andReturn(kafkaConfig).anyTimes()
      projectConfiguration.stateStoreConfig.andReturn(stateStoreConfigs).anyTimes()
      projectConfiguration.enableMetricPointPeriodReplacement.andReturn(true).anyTimes()
      projectConfiguration.enableStateStoreLogging.andReturn(true).anyTimes()
      projectConfiguration.loggingDelayInSeconds.andReturn(60).anyTimes()
    }
    EasyMock.replay(projectConfiguration)
    projectConfiguration
  }
}
