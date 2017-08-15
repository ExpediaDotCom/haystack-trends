package com.expedia.www.haystack.datapoints.aggregation.metrics

import com.expedia.www.haystack.datapoints.aggregation.metrics.Interval.Interval
import com.expedia.www.haystack.datapoints.entities.{DataPoint, MetricType}

class CountTrendMetric(interval: Interval) extends TrendMetric(interval) {

  var currentDataPoint: Option[DataPoint] = None
  var currentCount: Long = 0

  override def mapToDataPoints(windowEndTimestamp: Long = currentDataPoint.map(_.timestamp).getOrElse(System.currentTimeMillis())): List[DataPoint] = {
    currentDataPoint.map { datapoint =>
      val metricName = s"${datapoint.metric}-${interval.name}"
      List(DataPoint(metricName, MetricType.Aggregate, datapoint.tags, currentCount, windowEndTimestamp))
    }.getOrElse(List())
  }

  override def compute(dataPoint: DataPoint): CountTrendMetric = {
    currentCount += 1
    currentDataPoint = Some(dataPoint)
    this
  }
}
