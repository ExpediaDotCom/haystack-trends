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

package com.expedia.www.haystack.metricpoints.aggregation.metrics

import com.expedia.www.haystack.metricpoints.aggregation.metrics.HistogramMetric._
import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.entities.StatValue.StatValue
import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, MetricType, StatValue, TagKeys}
import org.HdrHistogram.IntHistogram


object HistogramMetric {
  def appendTags(metricPoint: MetricPoint, interval: Interval, statValue: StatValue): Map[String, String] = {
    metricPoint.tags + (TagKeys.INTERVAL_KEY -> interval.name , TagKeys.STATS_KEY -> statValue.toString)
  }
}

class HistogramMetric(interval: Interval) extends Metric(interval) {

  var latestMetricPoint: Option[MetricPoint] = None
  var histogram: IntHistogram = new IntHistogram(1000, 0)

  override def mapToMetricPoints(publishingTimestamp: Long): List[MetricPoint] = {
    latestMetricPoint.map {
      metricPoint => {
        List(
          MetricPoint(metricPoint.metric, MetricType.Gauge, appendTags(metricPoint, interval, StatValue.MEAN), histogram.getMean.toLong, publishingTimestamp),
          MetricPoint(metricPoint.metric, MetricType.Gauge, appendTags(metricPoint, interval, StatValue.MAX), histogram.getMaxValue, publishingTimestamp),
          MetricPoint(metricPoint.metric, MetricType.Gauge, appendTags(metricPoint, interval, StatValue.MIN), histogram.getMinValue, publishingTimestamp),
          MetricPoint(metricPoint.metric, MetricType.Gauge, appendTags(metricPoint, interval, StatValue.PERCENTILE_99), histogram.getValueAtPercentile(99), publishingTimestamp),
          MetricPoint(metricPoint.metric, MetricType.Gauge, appendTags(metricPoint, interval, StatValue.STDDEV), histogram.getStdDeviation.toLong, publishingTimestamp),
          MetricPoint(metricPoint.metric, MetricType.Gauge, appendTags(metricPoint, interval, StatValue.MEDIAN), histogram.getValueAtPercentile(50), publishingTimestamp)
        )
      }
    }.getOrElse(List())
  }

  override def compute(metricPoint: MetricPoint): HistogramMetric = {
    histogram.recordValue(metricPoint.value.toLong)
    latestMetricPoint = Some(metricPoint)
    this
  }
}
