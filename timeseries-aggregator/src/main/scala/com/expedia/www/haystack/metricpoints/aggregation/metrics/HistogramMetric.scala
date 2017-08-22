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
import com.expedia.www.haystack.metricpoints.entities.HistogramType.HistogramType
import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.entities.{HistogramType, MetricPoint, MetricType}
import org.HdrHistogram.IntHistogram


object HistogramMetric {
  def getMetricName(metricPoint: MetricPoint, interval: Interval, histogramType: HistogramType): String = {
    s"${metricPoint.metric}.${interval.name}.${histogramType}"
  }
}

class HistogramMetric(interval: Interval) extends Metric(interval) {

  var latestMetricPoint: Option[MetricPoint] = None
  var histogram: IntHistogram = new IntHistogram(1000, 0)

  override def mapToMetricPoints(windowEndTimestamp: Long): List[MetricPoint] = {
    HistogramType
    latestMetricPoint.map {
      metricPoint => {
        List(
          MetricPoint(getMetricName(metricPoint, interval, HistogramType.MEAN), MetricType.Histogram, metricPoint.tags, histogram.getMean.toLong, windowEndTimestamp),
          MetricPoint(getMetricName(metricPoint, interval, HistogramType.MAX), MetricType.Histogram, metricPoint.tags, histogram.getMaxValue, windowEndTimestamp),
          MetricPoint(getMetricName(metricPoint, interval, HistogramType.MIN), MetricType.Histogram, metricPoint.tags, histogram.getMinValue, windowEndTimestamp),
          MetricPoint(getMetricName(metricPoint, interval, HistogramType.PERCENTILE_99), MetricType.Histogram, metricPoint.tags, histogram.getValueAtPercentile(99), windowEndTimestamp),
          MetricPoint(getMetricName(metricPoint, interval, HistogramType.STDDEV), MetricType.Histogram, metricPoint.tags, histogram.getStdDeviation.toLong, windowEndTimestamp),
          MetricPoint(getMetricName(metricPoint, interval, HistogramType.MEDIAN), MetricType.Histogram, metricPoint.tags, histogram.getValueAtPercentile(50), windowEndTimestamp)
        )
      }
    }.getOrElse(List())
  }

  override def compute(metricPoint: MetricPoint): HistogramMetric = {
    histogram.recordValue(metricPoint.value)
    latestMetricPoint = Some(metricPoint)
    this
  }
}
