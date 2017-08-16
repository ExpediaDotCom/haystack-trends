package com.expedia.www.haystack.datapoints.aggregation.metrics

import com.expedia.www.haystack.datapoints.aggregation.metrics.Interval.Interval
import com.expedia.www.haystack.datapoints.entities.MetricType.MetricType
import com.expedia.www.haystack.datapoints.entities.{DataPoint, MetricType}

abstract class TrendMetric(interval: Interval) extends ReadOnlyMetric(interval) {

  def compute(value: DataPoint): TrendMetric

}

abstract class ReadOnlyMetric(interval: Interval) {
  def getMetricKey: String = {
    " "
  }


  def getMetricInterval: Interval = {
    interval
  }

  def mapToDataPoints(publishingTimestamp: Long = System.currentTimeMillis()): List[DataPoint]

}


object TrendMetricFactory {

  def getTrendMetric(metricType: MetricType, timeWindow: Interval): Option[TrendMetric] = {
    metricType match {
      case MetricType.Histogram => Some(new HistogramTrendMetric(timeWindow))
      case MetricType.Aggregate => Some(new CountTrendMetric(timeWindow))
      case _ => None
    }
  }
}

object Interval extends Enumeration {
  type Interval = IntervalVal

  def all: List[Interval] = {
    List(ONE_MINUTE, FIVE_MINUTE, FIFTEEN_MINUTE, ONE_HOUR)
  }

  sealed case class IntervalVal(name: String, timeInMs: Long) extends Val(name) {

  }

  val ONE_MINUTE = IntervalVal("1min", 60000)
  val FIVE_MINUTE = IntervalVal("5min", 300000)
  val FIFTEEN_MINUTE = IntervalVal("15min", 1500000)
  val ONE_HOUR = IntervalVal("1hour", 6000000)
}


case class TimeWindow(startTime: Long, endTime: Long) extends Ordered[TimeWindow] {


  override def compare(that: TimeWindow): Int = {
    this.startTime.compare(that.startTime)

  }
}

object TimeWindow {

  def apply(timestamp: Long, interval: Interval): TimeWindow = {
    val intervalTimeInMs = interval.timeInMs
    val windowStart = (timestamp / intervalTimeInMs) * intervalTimeInMs
    val windowEnd = windowStart + intervalTimeInMs
    TimeWindow(windowStart, windowEnd)
  }
}
