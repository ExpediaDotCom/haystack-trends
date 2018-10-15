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

import com.expedia.metrics.MetricData
import com.expedia.www.haystack.commons.entities.Interval.Interval
import com.expedia.www.haystack.commons.entities.{Interval, TagKeys}
import com.expedia.www.haystack.trends.aggregation.entities._
import com.expedia.www.haystack.trends.aggregation.metrics.HistogramMetric
import com.expedia.www.haystack.trends.feature.FeatureSpec
import org.HdrHistogram.{Histogram, IntHistogram}

class HistogramMetricSpec extends FeatureSpec {

  val DURATION_METRIC_NAME = "duration"
  val SUCCESS_METRIC_NAME = "success-spans"
  val INVALID_METRIC_NAME = "invalid_metric"
  val SERVICE_NAME = "dummy_service"
  val OPERATION_NAME = "dummy_operation"

  val keys = Map(TagKeys.OPERATION_NAME_KEY -> OPERATION_NAME,
    TagKeys.SERVICE_NAME_KEY -> SERVICE_NAME)

  feature("Creating a histogram metric") {
    scenario("should get gauge metric type and stats for valid durationric points") {

      Given("some duration Metric Data points")
      val durations = List(10, 140)
      val interval: Interval = Interval.ONE_MINUTE

      val metricDataList: List[MetricData] = durations.map(duration => getMetricData(DURATION_METRIC_NAME, keys, duration, currentTimeInSecs))

      When("get metric is constructed")
      val metric = new HistogramMetric(interval)

      When("MetricData points are processed")
      metricDataList.map(metricData => metric.compute(metricData))
      val histMetricDataList: List[MetricData] = metric.mapToMetricDataList(metricDataList.last.getMetricDefinition.getKey, getTagsFromMetricData(metricDataList.last), metricDataList.last.getTimestamp)


      Then("aggregated metric name should be the same as the MetricData points name")
      histMetricDataList
        .map(histMetricData =>
          histMetricData.getMetricDefinition.getKey shouldEqual metricDataList.head.getMetricDefinition.getKey)

      Then("aggregated metric should contain of original metric tags")
      histMetricDataList.foreach(histogramMetricData => {
        val tags = histogramMetricData.getMetricDefinition.getTags.getKv

        keys.foreach(IncomingMetricPointTag => {
          tags.get(IncomingMetricPointTag._1) should not be None
          tags.get(IncomingMetricPointTag._1) shouldEqual IncomingMetricPointTag._2
        })

      })

      Then("aggregated metric should contain the correct interval name in tags")
      histMetricDataList.map(histMetricData => {
        getTagsFromMetricData(histMetricData).get(TagKeys.INTERVAL_KEY) should not be null
        getTagsFromMetricData(histMetricData).get(TagKeys.INTERVAL_KEY) shouldEqual interval.name
      })

      Then("should return valid values for all stats types")
      val expectedHistogram: IntHistogram = new IntHistogram(Int.MaxValue, 0)
      metricDataList.foreach(metricPoint => expectedHistogram.recordValue(metricPoint.getValue.toLong))
      verifyHistogramMetricValues(histMetricDataList, expectedHistogram)
    }

    scenario("should filter out metric points larger than the Histogram maxValue") {

      Given("some duration Metric points")
      val maxTrackableValue = 2000
      val durations = List(10, maxTrackableValue + 1)
      val interval: Interval = Interval.ONE_MINUTE

      val metricDataList: List[MetricData] = durations.map(duration => getMetricData(DURATION_METRIC_NAME, keys, duration, currentTimeInSecs))

      When("get metric is constructed")
      val metric = new HistogramMetric(interval, new Histogram(maxTrackableValue, 2))

      When("MetricData points are processed")
      metricDataList.map(metricData => metric.compute(metricData))
      val histMetricDataList: List[MetricData] = metric.mapToMetricDataList(metricDataList.last.getMetricDefinition.getKey, getTagsFromMetricData(metricDataList.last), metricDataList.last.getTimestamp)


      Then("the max should be the only metric that was in the histogram boundaries")
      histMetricDataList.filter(m => "max".equals(getTagsFromMetricData(m).get("stat").toString)).head.getValue shouldEqual 10.0
    }

    scenario("should filter out metric points larger than the Histogram maxValue even for boundary float value") {

      Given("some duration Metric points")
      val maxTrackableValue = 2147483647
      val durations = List(10, maxTrackableValue.toFloat + 1) // toFloat is important since int max + 1 is not within int's precision
      val interval: Interval = Interval.ONE_MINUTE

      val metricDataList: List[MetricData] = durations.map(duration => getMetricData(DURATION_METRIC_NAME, keys, duration, currentTimeInSecs))

      When("get metric is constructed")
      val metric = new HistogramMetric(interval, new Histogram(maxTrackableValue, 2))

      When("MetricPoints are processed")
      metricDataList.map(metricData => metric.compute(metricData))
      val histMetricPoints: List[MetricData] = metric.mapToMetricDataList(metricDataList.last.getMetricDefinition.getKey, getTagsFromMetricData(metricDataList.last), metricDataList.last.getTimestamp)

      Then("the max should be the only metric that was in the histogram boundaries")
      histMetricPoints.filter(m => "max".equals(getTagsFromMetricData(m).get("stat").toString)).head.getValue shouldEqual 10.0
    }

    def verifyHistogramMetricValues(resultingMetricPoints: List[MetricData], expectedHistogram: IntHistogram) = {
      val resultingMetricPointsMap: Map[String, Float] =
        resultingMetricPoints.map(resultingMetricPoint => getTagsFromMetricData(resultingMetricPoint).get(TagKeys.STATS_KEY) -> resultingMetricPoint.getValue.toFloat).toMap

      resultingMetricPointsMap(StatValue.MEAN.toString) shouldEqual expectedHistogram.getMean.toLong
      resultingMetricPointsMap(StatValue.MAX.toString) shouldEqual expectedHistogram.getMaxValue
      resultingMetricPointsMap(StatValue.MIN.toString) shouldEqual expectedHistogram.getMinValue
      resultingMetricPointsMap(StatValue.PERCENTILE_95.toString) shouldEqual expectedHistogram.getValueAtPercentile(95)
      resultingMetricPointsMap(StatValue.PERCENTILE_99.toString) shouldEqual expectedHistogram.getValueAtPercentile(99)
      resultingMetricPointsMap(StatValue.STDDEV.toString) shouldEqual expectedHistogram.getStdDeviation.toLong
      resultingMetricPointsMap(StatValue.MEDIAN.toString) shouldEqual expectedHistogram.getValueAtPercentile(50)
    }
  }
}
