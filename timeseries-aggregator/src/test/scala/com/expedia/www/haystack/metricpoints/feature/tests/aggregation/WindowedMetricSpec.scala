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

package com.expedia.www.haystack.metricpoints.feature.tests.aggregation

import com.expedia.www.haystack.metricpoints.aggregation.WindowedMetric
import com.expedia.www.haystack.metricpoints.aggregation.metrics.HistogramMetric
import com.expedia.www.haystack.metricpoints.aggregation.rules.MetricRuleEngine
import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.entities.MetricType.MetricType
import com.expedia.www.haystack.metricpoints.entities.{HistogramStats, Interval, MetricPoint, MetricType}
import com.expedia.www.haystack.metricpoints.feature.FeatureSpec
import org.HdrHistogram.IntHistogram

class WindowedMetricSpec extends FeatureSpec with MetricRuleEngine {

  val DURATION_METRIC_NAME = "duration"
  val TOTAL_METRIC_NAME = "total-spans"
  val INVALID_METRIC_NAME = "invalid_metric"
  val SERVICE_NAME = "dummy_service"
  val OPERATION_NAME = "dummy_operation"

  object TagKeys {
    val OPERATION_NAME_KEY = "operationName"
    val SERVICE_NAME_KEY = "serviceName"
  }

  val keys = Map(TagKeys.OPERATION_NAME_KEY -> OPERATION_NAME,
    TagKeys.SERVICE_NAME_KEY -> SERVICE_NAME)

  feature("Creating a WindowedMetric") {
    scenario("should get aggregated MetricPoints post first Interval and second Interval") {

      Given("some duration MetricPoints")
      val durations: List[Long] = List(10, 140)
      val intervals: List[Interval] = List(Interval.ONE_MINUTE, Interval.FIFTEEN_MINUTE)

      val metricPoints: List[MetricPoint] = durations.map(duration => MetricPoint(DURATION_METRIC_NAME, MetricType.Metric, keys, duration, System.currentTimeMillis))
      val metricTypes: Set[MetricType] = metricPoints.map(metricPoint => findMatchingMetric(metricPoint)).toSet

      When("creating a WindowedMetric and passing some MetricPoints")
      val windowedMetric: WindowedMetric = new WindowedMetric(metricTypes.head, intervals, metricPoints(0))

      metricPoints.indices.foreach(i => if (i > 0) {
        windowedMetric.compute(metricPoints(i))
      })
      val aggregatedMetricPointsBefore: List[MetricPoint] = windowedMetric.getComputedMetricPoints

      val expectedHistogram: IntHistogram = new IntHistogram(1000, 0)
      metricPoints.map(metricPoint => expectedHistogram.recordValue(metricPoint.value))

      Then("should return 0 MetricPoints if we try to get it before interval")
      aggregatedMetricPointsBefore.size should be(0)

      When("adding a MetricPoint outside of first Interval")
      val newMetricPointAfterFirstInterval: MetricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Metric, keys, 80, System.currentTimeMillis + intervals.head.timeInMs)
      windowedMetric.compute(newMetricPointAfterFirstInterval)
      val aggregatedMetricPointsAfterFirstInterval: List[MetricPoint] = windowedMetric.getComputedMetricPoints

      Then("should return (1 interval * HistogramStats size) MetricPoints if we try to get it now")
      aggregatedMetricPointsAfterFirstInterval.size should be(1 * HistogramStats.maxId)

      Then("should return valid values for all histogram types")
      val aggregatedMetricPointsAfterFirstIntervalMap: Map[String, Long] = aggregatedMetricPointsAfterFirstInterval
        .map(aggMetricPoint =>
          aggMetricPoint.metric -> aggMetricPoint.value).toMap

      aggregatedMetricPointsAfterFirstIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, intervals.head, HistogramStats.MEAN)).get shouldEqual (expectedHistogram.getMean.toLong)
      aggregatedMetricPointsAfterFirstIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, intervals.head, HistogramStats.MAX)).get shouldEqual (expectedHistogram.getMaxValue)
      aggregatedMetricPointsAfterFirstIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, intervals.head, HistogramStats.MIN)).get shouldEqual (expectedHistogram.getMinValue)
      aggregatedMetricPointsAfterFirstIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, intervals.head, HistogramStats.PERCENTILE_99)).get shouldEqual (expectedHistogram.getValueAtPercentile(99))
      aggregatedMetricPointsAfterFirstIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, intervals.head, HistogramStats.STDDEV)).get shouldEqual (expectedHistogram.getStdDeviation.toLong)
      aggregatedMetricPointsAfterFirstIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, intervals.head, HistogramStats.MEDIAN)).get shouldEqual (expectedHistogram.getValueAtPercentile(50))

      When("adding a MetricPoint outside of Second Interval now")
      expectedHistogram.recordValue(newMetricPointAfterFirstInterval.value)
      val newMetricPointAfterSecondInterval: MetricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Metric, keys, 80, System.currentTimeMillis + intervals(1).timeInMs)
      windowedMetric.compute(newMetricPointAfterSecondInterval)
      val aggregatedMetricPointsAfterSecondInterval: List[MetricPoint] = windowedMetric.getComputedMetricPoints

      Then("should return (1 interval * HistogramStats size) MetricPoints if we try to get it now")
      aggregatedMetricPointsAfterSecondInterval.size should be(1 * HistogramStats.maxId)

      Then("should return valid values for all histogram types")
      val aggregatedMetricPointsAfterSecondIntervalMap: Map[String, Long] = aggregatedMetricPointsAfterSecondInterval
        .map(aggMetricPoint =>
          aggMetricPoint.metric -> aggMetricPoint.value).toMap

      aggregatedMetricPointsAfterSecondIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, intervals(1), HistogramStats.MEAN)).get shouldEqual (expectedHistogram.getMean.toLong)
      aggregatedMetricPointsAfterSecondIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, intervals(1), HistogramStats.MAX)).get shouldEqual (expectedHistogram.getMaxValue)
      aggregatedMetricPointsAfterSecondIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, intervals(1), HistogramStats.MIN)).get shouldEqual (expectedHistogram.getMinValue)
      aggregatedMetricPointsAfterSecondIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, intervals(1), HistogramStats.PERCENTILE_99)).get shouldEqual (expectedHistogram.getValueAtPercentile(99))
      aggregatedMetricPointsAfterSecondIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, intervals(1), HistogramStats.STDDEV)).get shouldEqual (expectedHistogram.getStdDeviation.toLong)
      aggregatedMetricPointsAfterSecondIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, intervals(1), HistogramStats.MEDIAN)).get shouldEqual (expectedHistogram.getValueAtPercentile(50))
    }

    scenario("should get aggregated MetricPoints post maximum Interval") {

      Given("some duration MetricPoints")
      val durations: List[Long] = List(10, 140, 250)
      val intervals: List[Interval] = List(Interval.ONE_MINUTE, Interval.FIFTEEN_MINUTE, Interval.ONE_HOUR)

      val metricPoints: List[MetricPoint] = durations.map(duration => MetricPoint(DURATION_METRIC_NAME, MetricType.Metric, keys, duration, System.currentTimeMillis))
      val metricTypes: Set[MetricType] = metricPoints.map(metricPoint => findMatchingMetric(metricPoint)).toSet

      val expectedHistogram: IntHistogram = new IntHistogram(1000, 0)
      metricPoints.map(metricPoint => expectedHistogram.recordValue(metricPoint.value))

      When("creating a WindowedMetric and passing some MetricPoints")
      val windowedMetric: WindowedMetric = new WindowedMetric(metricTypes.head, intervals, metricPoints(0))

      metricPoints.indices.foreach(i => if (i > 0) {
        windowedMetric.compute(metricPoints(i))
      })

      When("adding a MetricPoint outside of max Interval")
      val newMetricPointAfterMaxInterval: MetricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Metric, keys, 80, System.currentTimeMillis + intervals.last.timeInMs)
      windowedMetric.compute(newMetricPointAfterMaxInterval)
      val aggregatedMetricPointsAfterMaxInterval: List[MetricPoint] = windowedMetric.getComputedMetricPoints

      Then("should return (1 interval * HistogramStats size) MetricPoints if we try to get it now")
      aggregatedMetricPointsAfterMaxInterval.size should be(intervals.size * HistogramStats.maxId)

      Then("should return valid values for all histogram types")
      val aggregatedMetricPointsAfterMaxIntervalMap: Map[String, Long] = aggregatedMetricPointsAfterMaxInterval
        .map(aggMetricPoint =>
          aggMetricPoint.metric -> aggMetricPoint.value).toMap

      intervals.map(interval => {
        aggregatedMetricPointsAfterMaxIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, interval, HistogramStats.MEAN)).get shouldEqual (expectedHistogram.getMean.toLong)
        aggregatedMetricPointsAfterMaxIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, interval, HistogramStats.MAX)).get shouldEqual (expectedHistogram.getMaxValue)
        aggregatedMetricPointsAfterMaxIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, interval, HistogramStats.MIN)).get shouldEqual (expectedHistogram.getMinValue)
        aggregatedMetricPointsAfterMaxIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, interval, HistogramStats.PERCENTILE_99)).get shouldEqual (expectedHistogram.getValueAtPercentile(99))
        aggregatedMetricPointsAfterMaxIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, interval, HistogramStats.STDDEV)).get shouldEqual (expectedHistogram.getStdDeviation.toLong)
        aggregatedMetricPointsAfterMaxIntervalMap.get(HistogramMetric.getMetricName(metricPoints.head, interval, HistogramStats.MEDIAN)).get shouldEqual (expectedHistogram.getValueAtPercentile(50))
      })
    }
  }
}
