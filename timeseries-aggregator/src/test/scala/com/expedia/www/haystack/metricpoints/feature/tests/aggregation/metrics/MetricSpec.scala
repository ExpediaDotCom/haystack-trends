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
import com.expedia.www.haystack.metricpoints.entities.{HistogramType, Interval, MetricPoint, MetricType}
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
      val duration1: Long = 10
      val duration2: Long = 140
      val interval: Interval = Interval.ONE_MINUTE

      val metricPoint1: MetricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Metric, keys, duration1, System.currentTimeMillis)
      val metricPoint2: MetricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Metric, keys, duration2, System.currentTimeMillis)
      val metricType1: MetricType = findMatchingMetric(metricPoint1)
      val metricType2: MetricType = findMatchingMetric(metricPoint2)

      When("get Metric using MetricFactory")
      val metric: Metric = MetricFactory.getMetric(metricType1, interval).get

      Then("should get Histogram MetricType")
      metric should not be (None)
      metricType1 shouldEqual (metricType2)
      metric.getClass.getName shouldEqual (classOf[HistogramMetric].getName)

      When("MetricPoints are processed")
      metric.compute(metricPoint1)
      metric.compute(metricPoint2)
      val histMetricPoints: List[MetricPoint] = metric.mapToMetricPoints()

      val expectedHistogram: IntHistogram = new IntHistogram(1000, 0)
      expectedHistogram.recordValue(metricPoint1.value)
      expectedHistogram.recordValue(metricPoint2.value)

      Then("aggregated metric name should consist of original metric name as well as interval")
      histMetricPoints
        .map(histMetricPoint =>
          (histMetricPoint.metric.contains(metricPoint1.metric) && histMetricPoint.metric.contains(interval.name)) shouldEqual (true))

      Then("should return valid values for all histogram types")
      val histMetricPointsMap: Map[String, Long] = histMetricPoints
        .map(histMetricPoint =>
          histMetricPoint.metric -> histMetricPoint.value).toMap

      histMetricPointsMap.get(HistogramMetric.getMetricName(metricPoint1, interval, HistogramType.MEAN)).get shouldEqual (expectedHistogram.getMean.toLong)
      histMetricPointsMap.get(HistogramMetric.getMetricName(metricPoint1, interval, HistogramType.MAX)).get shouldEqual (expectedHistogram.getMaxValue)
      histMetricPointsMap.get(HistogramMetric.getMetricName(metricPoint1, interval, HistogramType.MIN)).get shouldEqual (expectedHistogram.getMinValue)
      histMetricPointsMap.get(HistogramMetric.getMetricName(metricPoint1, interval, HistogramType.PERCENTILE_99)).get shouldEqual (expectedHistogram.getValueAtPercentile(99))
      histMetricPointsMap.get(HistogramMetric.getMetricName(metricPoint1, interval, HistogramType.STDDEV)).get shouldEqual (expectedHistogram.getStdDeviation.toLong)
      histMetricPointsMap.get(HistogramMetric.getMetricName(metricPoint1, interval, HistogramType.MEDIAN)).get shouldEqual (expectedHistogram.getValueAtPercentile(50))
    }

    scenario("should get Count MetricType for Total MetricPoint") {

      Given("some 'total-spans' metric points")
      val interval: Interval = Interval.FIFTEEN_MINUTE

      val metricPoint1: MetricPoint = MetricPoint(TOTAL_METRIC_NAME, MetricType.Metric, keys, 1, System.currentTimeMillis)
      val metricPoint2: MetricPoint = MetricPoint(TOTAL_METRIC_NAME, MetricType.Metric, keys, 1, System.currentTimeMillis)
      val metricPoint3: MetricPoint = MetricPoint(TOTAL_METRIC_NAME, MetricType.Metric, keys, 1, System.currentTimeMillis)

      val metricType1: MetricType = findMatchingMetric(metricPoint1)
      val metricType2: MetricType = findMatchingMetric(metricPoint2)
      val metricType3: MetricType = findMatchingMetric(metricPoint3)

      When("getting Metric using MetricFactory")
      val metric: Metric = MetricFactory.getMetric(metricType1, interval).get

      Then("should get CountMetric MetricType")
      metric should not be (None)
      metricType1 shouldEqual metricType2
      metricType2 shouldEqual metricType3
      metric.getClass.getName shouldEqual (classOf[CountMetric].getName)

      When("MetricPoints are processed")
      metric.compute(metricPoint1)
      metric.compute(metricPoint2)
      metric.compute(metricPoint3)
      val countMetricPoints: List[MetricPoint] = metric.mapToMetricPoints()

      Then("aggregated metric name should consist of original metric name as well as interval1")
      countMetricPoints
        .map(countMetricPoint =>
          (countMetricPoint.metric.contains(metricPoint1.metric) && countMetricPoint.metric.contains(interval.name)) shouldEqual (true))

      Then("should return valid aggregated value for count")
      val countMetricPointsMap: Map[String, Long] = countMetricPoints
        .map(histMetricPoint =>
          histMetricPoint.metric -> histMetricPoint.value).toMap

      countMetricPointsMap.get(CountMetric.getMetricName(metricPoint1, interval)).get shouldEqual (3)
    }
  }
}
