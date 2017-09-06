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

import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, MetricType, StatValue}
import org.HdrHistogram.IntHistogram


class HistogramMetric(interval: Interval) extends Metric(interval) {

  var latestMetricPoint: Option[MetricPoint] = None
  var histogram: IntHistogram = new IntHistogram(Int.MaxValue, 0)

  override def mapToMetricPoints(publishingTimestamp: Long): List[MetricPoint] = {
    import StatValue._
    latestMetricPoint match {
      case Some(metricPoint) =>
        val result = Map(
          MEAN -> histogram.getMean.toLong,
          MIN -> histogram.getMinValue,
          PERCENTILE_99 -> histogram.getValueAtPercentile(99),
          STDDEV -> histogram.getStdDeviation.toLong,
          MEDIAN -> histogram.getValueAtPercentile(50),
          MAX -> histogram.getMaxValue
        ).map {
          case (stat, value) =>
            MetricPoint(metricPoint.metric, MetricType.Gauge, appendTags(metricPoint, interval, stat), value, publishingTimestamp)
        }
        result.toList

      case None => List()
    }
  }

  override def compute(metricPoint: MetricPoint): HistogramMetric = {
    histogram.recordValue(metricPoint.value.toLong)
    latestMetricPoint = Some(metricPoint)
    this
  }
}


object HistogramMetricFactory extends MetricFactory {
  override def createMetric(interval: Interval): Metric = new HistogramMetric(interval)
}
