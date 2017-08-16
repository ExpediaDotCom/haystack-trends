package com.expedia.www.haystack.metricpoints.aggregation.metrics

import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.entities.MetricType.MetricType
import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, MetricType}

abstract class Metric(interval: Interval) {

  def compute(value: MetricPoint): Metric

  def getMetricKey: String = {
    " "
  }

  def getMetricInterval: Interval = {
    interval
  }

  def mapToMetricPoints(publishingTimestamp: Long = System.currentTimeMillis()): List[MetricPoint]

}


object MetricFactory {

  def getMetric(metricType: MetricType, timeWindow: Interval): Option[Metric] = {
    metricType match {
      case MetricType.Histogram => Some(new HistogramMetric(timeWindow))
      case MetricType.Aggregate => Some(new CountMetric(timeWindow))
      case _ => None
    }
  }
}

