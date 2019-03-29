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

package com.expedia.www.haystack.trends.aggregation.metrics

import java._

import com.codahale.metrics.Timer
import com.expedia.metrics.{MetricData, MetricDefinition, TagCollection}
import com.expedia.www.haystack.commons.entities.Interval.Interval
import com.expedia.www.haystack.trends.aggregation.metrics.AggregationType.AggregationType
import com.expedia.www.haystack.trends.config.AppConfiguration
import com.expedia.www.haystack.trends.kstream.serde.metric.{HistogramMetricSerde, MetricSerde}
import org.HdrHistogram.Histogram


/**
  * This is a base metric which can compute the histogram of the given events. It uses  hdr histogram(https://github.com/HdrHistogram/HdrHistogram) internally to compute the histogram
  *
  * @param interval  : interval for the metric
  * @param histogram : current histogram, the current histogram should be a new histogram object for a new metric but can be passed when we want to restore a given metric after the application crashed
  */
class HistogramMetric(interval: Interval, histogram: Histogram) extends Metric(interval) {

  private val HistogramMetricComputeTimer: Timer = metricRegistry.timer("histogram.metric.compute.time")

  def this(interval: Interval) = this(interval, new Histogram(AppConfiguration.histogramMetricConfiguration.maxValue, AppConfiguration.histogramMetricConfiguration.precision))


  override def mapToMetricDataList(metricKey: String, tags: util.Map[String, String], publishingTimestamp: Long): List[MetricData] = {
    import com.expedia.www.haystack.trends.aggregation.entities.StatValue._
    histogram.getTotalCount match {
      case 0 => List()
      case _ => val result = Map(
        MEAN -> histogram.getMean.toLong,
        MIN -> histogram.getMinValue,
        PERCENTILE_95 -> histogram.getValueAtPercentile(95),
        PERCENTILE_99 -> histogram.getValueAtPercentile(99),
        STDDEV -> histogram.getStdDeviation.toLong,
        MEDIAN -> histogram.getValueAtPercentile(50),
        MAX -> histogram.getMaxValue
      ).map {
        case (stat, value) =>
          val tagCollection = new TagCollection(appendTags(tags, interval, stat))
          val metricDefinition = new MetricDefinition(metricKey, tagCollection, TagCollection.EMPTY)
          new MetricData(metricDefinition, value, publishingTimestamp)
      }
        result.toList
    }
  }

  def getRunningHistogram: Histogram = {
    histogram
  }

  override def compute(metricData: MetricData): HistogramMetric = {
    if (metricData.getValue.toLong <= histogram.getHighestTrackableValue) {
      val timerContext = HistogramMetricComputeTimer.time()
      histogram.recordValue(metricData.getValue.toLong)
      timerContext.close()
    } else {
      val timerContext = HistogramMetricComputeTimer.time()
      histogram.recordValue(histogram.getHighestTrackableValue)
      timerContext.close()
    }
    this
  }

}


object HistogramMetricFactory extends MetricFactory {

  override def createMetric(interval: Interval): HistogramMetric = new HistogramMetric(interval)

  override def getAggregationType: AggregationType = AggregationType.Histogram

  override def getMetricSerde: MetricSerde = HistogramMetricSerde
}
