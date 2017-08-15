package com.expedia.www.haystack.datapoints.aggregation.metrics

import com.expedia.www.haystack.datapoints.aggregation.metrics.Interval.Interval
import com.expedia.www.haystack.datapoints.entities.{DataPoint, MetricType}
import org.HdrHistogram.IntHistogram

class HistogramTrendMetric(interval: Interval) extends TrendMetric(interval) {

  var currentDataPoint: Option[DataPoint] = None
  var histogram: IntHistogram = new IntHistogram(java.lang.Long.MAX_VALUE, 0)

  override def mapToDataPoints(windowEndTimestamp: Long): List[DataPoint] = {
    currentDataPoint.map {
      dataPoint => {
        List {
          DataPoint(s"${dataPoint.metric}-mean-${interval.name}", MetricType.Histogram, dataPoint.tags, histogram.getMean.toLong, windowEndTimestamp)
          DataPoint(s"${dataPoint.metric}-max-${interval.name}", MetricType.Histogram, dataPoint.tags, histogram.getMaxValue, windowEndTimestamp)
          DataPoint(s"${dataPoint.metric}-min-${interval.name}", MetricType.Histogram, dataPoint.tags, histogram.getMinValue, windowEndTimestamp)
          DataPoint(s"${dataPoint.metric}-99percentile-${interval.name}", MetricType.Histogram, dataPoint.tags, histogram.getValueAtPercentile(99), windowEndTimestamp)
          DataPoint(s"${dataPoint.metric}-stddev-${interval.name}", MetricType.Histogram, dataPoint.tags, histogram.getStdDeviation.toLong, windowEndTimestamp)
          DataPoint(s"${dataPoint.metric}.median-${interval.name}", MetricType.Histogram, dataPoint.tags, histogram.getValueAtPercentile(50), windowEndTimestamp)
        }
      }
    }.getOrElse(List())
  }

  override def compute(dataPoint: DataPoint): HistogramTrendMetric = {
    histogram.recordValue(dataPoint.value)
    currentDataPoint = Some(dataPoint)
    this
  }
}
