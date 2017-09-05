package com.expedia.www.haystack.metricpoints.entities

import com.expedia.www.haystack.metricpoints.entities.Interval.Interval

case class TimeWindow(startTime: Long, endTime: Long) extends Ordered[TimeWindow] {


  override def compare(that: TimeWindow): Int = {
    this.startTime.compare(that.startTime)
  }
}

object TimeWindow {

  def apply(timestamp: Long, interval: Interval): TimeWindow = {
    val intervalTimeInSeconds = interval.timeInSeconds
    val windowStart = (timestamp / intervalTimeInSeconds) * intervalTimeInSeconds
    val windowEnd = windowStart + intervalTimeInSeconds
    TimeWindow(windowStart, windowEnd)
  }
}

object Interval extends Enumeration {
  type Interval = IntervalVal
  val ONE_MINUTE = IntervalVal("OneMinute", 60)
  val FIVE_MINUTE = IntervalVal("FiveMinute", 300)
  val FIFTEEN_MINUTE = IntervalVal("FifteenMinute", 900)
  val ONE_HOUR = IntervalVal("OneHour", 3600)

  def all: List[Interval] = {
    List(ONE_MINUTE, FIVE_MINUTE, FIFTEEN_MINUTE, ONE_HOUR)
  }

  sealed case class IntervalVal(name: String, timeInSeconds: Long) extends Val(name) {
  }
}
