package com.expedia.www.haystack.metricpoints.aggregation.metrics

import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, MetricType}
import org.HdrHistogram.IntHistogram

class HistogramMetric(interval: Interval) extends Metric(interval) {

  var latestMetricPoint: Option[MetricPoint] = None
  var histogram: IntHistogram = new IntHistogram(java.lang.Long.MAX_VALUE, 0)

  override def mapToMetricPoints(windowEndTimestamp: Long): List[MetricPoint] = {
    latestMetricPoint.map {
      metricPoint => {
        List {
          MetricPoint(s"${metricPoint.metric}-mean-${interval.name}", MetricType.Histogram, metricPoint.tags, histogram.getMean.toLong, windowEndTimestamp)
          MetricPoint(s"${metricPoint.metric}-max-${interval.name}", MetricType.Histogram, metricPoint.tags, histogram.getMaxValue, windowEndTimestamp)
          MetricPoint(s"${metricPoint.metric}-min-${interval.name}", MetricType.Histogram, metricPoint.tags, histogram.getMinValue, windowEndTimestamp)
          MetricPoint(s"${metricPoint.metric}-99percentile-${interval.name}", MetricType.Histogram, metricPoint.tags, histogram.getValueAtPercentile(99), windowEndTimestamp)
          MetricPoint(s"${metricPoint.metric}-stddev-${interval.name}", MetricType.Histogram, metricPoint.tags, histogram.getStdDeviation.toLong, windowEndTimestamp)
          MetricPoint(s"${metricPoint.metric}.median-${interval.name}", MetricType.Histogram, metricPoint.tags, histogram.getValueAtPercentile(50), windowEndTimestamp)
        }
      }
    }.getOrElse(List())
  }

  override def compute(metricPoint: MetricPoint): HistogramMetric = {
    histogram.recordValue(metricPoint.value)
    latestMetricPoint = Some(metricPoint)
    this
  }
}
