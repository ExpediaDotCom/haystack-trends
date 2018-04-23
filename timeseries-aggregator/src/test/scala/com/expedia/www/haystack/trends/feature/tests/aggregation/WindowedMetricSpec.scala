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

package com.expedia.www.haystack.trends.feature.tests.aggregation

import com.expedia.www.haystack.commons.entities.Interval.Interval
import com.expedia.www.haystack.commons.entities.{Interval, MetricPoint, MetricType}
import com.expedia.www.haystack.trends.aggregation.WindowedMetric
import com.expedia.www.haystack.trends.aggregation.metrics.{CountMetric, HistogramMetric, HistogramMetricFactory}
import com.expedia.www.haystack.trends.feature.FeatureSpec

class WindowedMetricSpec extends FeatureSpec {

  val DURATION_METRIC_NAME = "duration"
  val TOTAL_METRIC_NAME = "total-spans"
  val INVALID_METRIC_NAME = "invalid_metric"
  val SERVICE_NAME = "dummy_service"
  val OPERATION_NAME = "dummy_operation"
  val keys = Map(TagKeys.OPERATION_NAME_KEY -> OPERATION_NAME,
    TagKeys.SERVICE_NAME_KEY -> SERVICE_NAME)

  object TagKeys {
    val OPERATION_NAME_KEY = "operationName"
    val SERVICE_NAME_KEY = "serviceName"
  }

  feature("Creating a WindowedMetric") {

    scenario("should get aggregated MetricPoints post watermarked metrics") {

      Given("some duration MetricPoints")
      val durations: List[Long] = List(10, 140)
      val intervals: List[Interval] = List(Interval.ONE_MINUTE, Interval.FIFTEEN_MINUTE)

      val metricPoints: List[MetricPoint] = durations.map(duration => MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, keys, duration, currentTimeInSecs))

      When("creating a WindowedMetric and passing some MetricPoints and aggregation type as Histogram")
      val windowedMetric: WindowedMetric = WindowedMetric.createWindowedMetric(metricPoints.head, HistogramMetricFactory, watermarkedWindows = 1, Interval.ONE_MINUTE)

      metricPoints.indices.foreach(i => if (i > 0) {
        windowedMetric.compute(metricPoints(i))
      })

      val expectedMetric: HistogramMetric = new HistogramMetric(Interval.ONE_MINUTE)
      metricPoints.foreach(metricPoint => expectedMetric.compute(metricPoint))

      Then("should return 0 MetricPoints if we try to get it before interval")
      val aggregatedMetricPointsBefore: List[MetricPoint] = windowedMetric.getComputedMetricPoints(metricPoints.last)
      aggregatedMetricPointsBefore.size shouldBe 0

      When("adding a MetricPoint outside of first Interval")
      val newMetricPointAfterFirstInterval: MetricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, keys, 80, currentTimeInSecs + intervals.head.timeInSeconds)

      windowedMetric.compute(newMetricPointAfterFirstInterval)

      val aggregatedMetricPointsAfterFirstInterval: List[MetricPoint] = windowedMetric.getComputedMetricPoints(metricPoints.last)

      //Have to fix dev code and then all the validation test
      Then("should return the metric points for the previous interval")


      When("adding a MetricPoint outside of second interval now")
      expectedMetric.compute(newMetricPointAfterFirstInterval)
      val newMetricPointAfterSecondInterval: MetricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, keys, 80, currentTimeInSecs + intervals(1).timeInSeconds)
      windowedMetric.compute(newMetricPointAfterSecondInterval)
      val aggregatedMetricPointsAfterSecondInterval: List[MetricPoint] = windowedMetric.getComputedMetricPoints(metricPoints.last)

      //Have to fix dev code and then all the validation test
      Then("should return the metric points for the second interval")
    }

    scenario("should get aggregated MetricPoints post maximum Interval") {

      Given("some duration MetricPoints")
      val durations: List[Long] = List(10, 140, 250)
      val intervals: List[Interval] = List(Interval.ONE_MINUTE, Interval.FIFTEEN_MINUTE, Interval.ONE_HOUR)

      val metricPoints: List[MetricPoint] = durations.map(duration => MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, keys, duration, currentTimeInSecs))


      When("creating a WindowedMetric and passing some MetricPoints")
      val windowedMetric: WindowedMetric = WindowedMetric.createWindowedMetric(metricPoints.head, HistogramMetricFactory, watermarkedWindows = 1, Interval.ONE_MINUTE)

      metricPoints.indices.foreach(i => if (i > 0) {
        windowedMetric.compute(metricPoints(i))
      })

      When("adding a MetricPoint outside of max Interval")
      val newMetricPointAfterMaxInterval: MetricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, keys, 80, currentTimeInSecs + intervals.last.timeInSeconds)
      windowedMetric.compute(newMetricPointAfterMaxInterval)
      val aggregatedMetricPointsAfterMaxInterval: List[MetricPoint] = windowedMetric.getComputedMetricPoints(metricPoints.last)

      Then("should return valid values for all count intervals")

      val expectedOneMinuteMetric: CountMetric = new CountMetric(Interval.ONE_MINUTE)
      metricPoints.foreach(metricPoint => expectedOneMinuteMetric.compute(metricPoint))

      val expectedFifteenMinuteMetric: CountMetric = new CountMetric(Interval.FIFTEEN_MINUTE)
      metricPoints.foreach(metricPoint => expectedFifteenMinuteMetric.compute(metricPoint))

      val expectedOneHourMetric: CountMetric = new CountMetric(Interval.ONE_HOUR)
      metricPoints.foreach(metricPoint => expectedOneHourMetric.compute(metricPoint))
    }
  }
}
