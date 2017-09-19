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

package com.expedia.www.haystack.trends.feature.tests.aggregation.metrics

import com.expedia.www.haystack.trends.aggregation.metrics.HistogramMetric
import com.expedia.www.haystack.trends.commons.entities.{MetricPoint, MetricType, TagKeys}
import com.expedia.www.haystack.trends.entities.Interval.Interval
import com.expedia.www.haystack.trends.entities._
import com.expedia.www.haystack.trends.feature.FeatureSpec
import org.HdrHistogram.IntHistogram

class HistogramMetricSpec extends FeatureSpec {

  val DURATION_METRIC_NAME = "duration"
  val TOTAL_METRIC_NAME = "total-spans"
  val INVALID_METRIC_NAME = "invalid_metric"
  val SERVICE_NAME = "dummy_service"
  val OPERATION_NAME = "dummy_operation"

  val keys = Map(TagKeys.OPERATION_NAME_KEY -> OPERATION_NAME,
    TagKeys.SERVICE_NAME_KEY -> SERVICE_NAME)

  feature("Creating a histogram metric") {
    scenario("should get gauge metric type and stats for valid durationric points") {

      Given("some duration Metric points")
      val durations = List(10, 140)
      val interval: Interval = Interval.ONE_MINUTE

      val metricPoints: List[MetricPoint] = durations.map(duration => MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, keys, duration, currentTimeInSecs))

      When("get metric is constructed")
      val metric = new HistogramMetric(interval)

      When("MetricPoints are processed")
      metricPoints.map(metricPoint => metric.compute(metricPoint))
      val histMetricPoints: List[MetricPoint] = metric.mapToMetricPoints(metricPoints.last.epochTimeInSeconds)


      Then("aggregated metric name should be the same as the MetricPoints name")
      histMetricPoints
        .map(histMetricPoint =>
          histMetricPoint.metric shouldEqual metricPoints.head.metric)

      Then("aggregated metric should contain of original metric tags")
      histMetricPoints.foreach(histogramMetricPoint => {
        val tags = histogramMetricPoint.tags

        keys.foreach(IncomingMetricPointTag => {
          tags.get(IncomingMetricPointTag._1) should not be None
          tags.get(IncomingMetricPointTag._1) shouldEqual Some(IncomingMetricPointTag._2)
        })

      })

      Then("aggregated metric should contain the correct interval name in tags")
      histMetricPoints.map(histMetricPoint => {
        histMetricPoint.tags.get(TagKeys.INTERVAL_KEY) should not be None
        histMetricPoint.tags(TagKeys.INTERVAL_KEY) shouldEqual interval.name
      })

      Then("should return valid values for all stats types")
      val expectedHistogram: IntHistogram = new IntHistogram(Int.MaxValue, 0)
      metricPoints.foreach(metricPoint => expectedHistogram.recordValue(metricPoint.value.toLong))
      verifyHistogramMetricValues(histMetricPoints, expectedHistogram)
    }


    def verifyHistogramMetricValues(resultingMetricPoints: List[MetricPoint], expectedHistogram: IntHistogram) = {
      val resultingMetricPointsMap: Map[String, Float] =
        resultingMetricPoints.map(resultingMetricPoint => resultingMetricPoint.tags(TagKeys.STATS_KEY) -> resultingMetricPoint.value).toMap

      resultingMetricPointsMap(StatValue.MEAN.toString) shouldEqual expectedHistogram.getMean.toLong
      resultingMetricPointsMap(StatValue.MAX.toString) shouldEqual expectedHistogram.getMaxValue
      resultingMetricPointsMap(StatValue.MIN.toString) shouldEqual expectedHistogram.getMinValue
      resultingMetricPointsMap(StatValue.PERCENTILE_99.toString) shouldEqual expectedHistogram.getValueAtPercentile(99)
      resultingMetricPointsMap(StatValue.STDDEV.toString) shouldEqual expectedHistogram.getStdDeviation.toLong
      resultingMetricPointsMap(StatValue.MEDIAN.toString) shouldEqual expectedHistogram.getValueAtPercentile(50)
    }
  }
}
