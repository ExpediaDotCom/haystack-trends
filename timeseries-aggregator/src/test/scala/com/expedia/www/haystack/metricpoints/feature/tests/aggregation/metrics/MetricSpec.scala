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

package com.expedia.www.haystack.metricpoints.feature.tests.aggregation.metrics

import com.expedia.www.haystack.metricpoints.aggregation.metrics.{CountMetric, HistogramMetric, Metric, MetricFactory}
import com.expedia.www.haystack.metricpoints.aggregation.rules.MetricRuleEngine
import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.entities.MetricType.MetricType
import com.expedia.www.haystack.metricpoints.entities.{HistogramStats, Interval, MetricPoint, MetricType}
import com.expedia.www.haystack.metricpoints.feature.FeatureSpec
import org.HdrHistogram.IntHistogram

class MetricSpec extends FeatureSpec with MetricRuleEngine {

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

  feature("Creating a Metric (Count / Histogram") {
    scenario("should get Histogram MetricType and HistogramTypes for duration MetricPoints") {

      Given("some duration MetricPoints")
      val durations = List(10, 140)
      val interval: Interval = Interval.ONE_MINUTE

      val metricPoints: List[MetricPoint] = durations.map(duration => MetricPoint(DURATION_METRIC_NAME, MetricType.Metric, keys, duration, System.currentTimeMillis))
      val metricTypes: Set[MetricType] = metricPoints.map(metricPoint => findMatchingMetric(metricPoint)).toSet

      When("get Metric using MetricFactory")
      val metric: Metric = MetricFactory.getMetric(metricTypes.head, interval).get

      Then("should get Histogram MetricType")
      metric should not be (None)
      metricTypes.size shouldEqual (1)
      metric.getClass.getName shouldEqual (classOf[HistogramMetric].getName)

      When("MetricPoints are processed")
      metricPoints.map(metricPoint => metric.compute(metricPoint))
      val histMetricPoints: List[MetricPoint] = metric.mapToMetricPoints()

      val expectedHistogram: IntHistogram = new IntHistogram(1000, 0)
      metricPoints.map(metricPoint => expectedHistogram.recordValue(metricPoint.value))

      Then("aggregated metric name should consist of original metric name as well as interval")
      histMetricPoints
        .map(histMetricPoint =>
          (histMetricPoint.metric.contains(metricPoints.head.metric) && histMetricPoint.metric.contains(interval.name)) shouldEqual (true))

      Then("should return valid values for all histogram types")
      val histMetricPointsMap: Map[String, Long] = histMetricPoints
        .map(histMetricPoint =>
          histMetricPoint.metric -> histMetricPoint.value).toMap

      histMetricPointsMap.get(HistogramMetric.getMetricName(metricPoints.head, interval, HistogramStats.MEAN)).get shouldEqual (expectedHistogram.getMean.toLong)
      histMetricPointsMap.get(HistogramMetric.getMetricName(metricPoints.head, interval, HistogramStats.MAX)).get shouldEqual (expectedHistogram.getMaxValue)
      histMetricPointsMap.get(HistogramMetric.getMetricName(metricPoints.head, interval, HistogramStats.MIN)).get shouldEqual (expectedHistogram.getMinValue)
      histMetricPointsMap.get(HistogramMetric.getMetricName(metricPoints.head, interval, HistogramStats.PERCENTILE_99)).get shouldEqual (expectedHistogram.getValueAtPercentile(99))
      histMetricPointsMap.get(HistogramMetric.getMetricName(metricPoints.head, interval, HistogramStats.STDDEV)).get shouldEqual (expectedHistogram.getStdDeviation.toLong)
      histMetricPointsMap.get(HistogramMetric.getMetricName(metricPoints.head, interval, HistogramStats.MEDIAN)).get shouldEqual (expectedHistogram.getValueAtPercentile(50))
    }

    scenario("should get Count MetricType for Total MetricPoint") {

      Given("some 'total-spans' metric points")
      val interval: Interval = Interval.FIFTEEN_MINUTE

      val metricPoints = List(MetricPoint(TOTAL_METRIC_NAME, MetricType.Metric, keys, 1, System.currentTimeMillis),
        MetricPoint(TOTAL_METRIC_NAME, MetricType.Metric, keys, 1, System.currentTimeMillis),
        MetricPoint(TOTAL_METRIC_NAME, MetricType.Metric, keys, 1, System.currentTimeMillis))

      val metricTypes: Set[MetricType] = metricPoints.map(metricPoint => findMatchingMetric(metricPoint)).toSet

      When("getting Metric using MetricFactory")
      val metric: Metric = MetricFactory.getMetric(metricTypes.head, interval).get

      Then("should get CountMetric MetricType")
      metric should not be (None)
      metricTypes.size shouldEqual (1)
      metric.getClass.getName shouldEqual (classOf[CountMetric].getName)

      When("MetricPoints are processed")
      metricPoints.map(metricPoint => metric.compute(metricPoint))
      val countMetricPoints: List[MetricPoint] = metric.mapToMetricPoints()

      Then("aggregated metric name should consist of original metric name as well as interval1")
      countMetricPoints
        .map(countMetricPoint =>
          (countMetricPoint.metric.contains(metricPoints.head.metric) && countMetricPoint.metric.contains(interval.name)) shouldEqual (true))

      Then("should return valid aggregated value for count")
      val countMetricPointsMap: Map[String, Long] = countMetricPoints
        .map(histMetricPoint =>
          histMetricPoint.metric -> histMetricPoint.value).toMap

      countMetricPointsMap.get(CountMetric.getMetricName(metricPoints.head, interval)).get shouldEqual (3)
    }
  }
}
