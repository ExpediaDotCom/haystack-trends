package com.expedia.www.haystack.metricpoints.aggregation.metrics


import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, MetricType}

class CountMetric(interval: Interval) extends Metric(interval) {

  var latestMetricPoint: Option[MetricPoint] = None
  var currentCount: Long = 0

  override def mapToMetricPoints(windowEndTimestamp: Long = latestMetricPoint.map(_.timestamp).getOrElse(System.currentTimeMillis())): List[MetricPoint] = {
    latestMetricPoint.map { metricPoint =>
      val metricName = s"${metricPoint.metric}-${interval.name}"
      List(MetricPoint(metricName, MetricType.Aggregate, metricPoint.tags, currentCount, windowEndTimestamp))
    }.getOrElse(List())
  }

  override def compute(metricPoint: MetricPoint): CountMetric = {
    currentCount += 1
    latestMetricPoint = Some(metricPoint)
    this
  }
}
