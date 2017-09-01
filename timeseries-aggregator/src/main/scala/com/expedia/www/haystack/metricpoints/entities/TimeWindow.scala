package com.expedia.www.haystack.metricpoints.entities

import com.expedia.www.haystack.metricpoints.entities.Interval.Interval

case class TimeWindow(startTime: Long, endTime: Long) extends Ordered[TimeWindow] {


  override def compare(that: TimeWindow): Int = {
    this.startTime.compare(that.startTime)
  }
}

object TimeWindow {

  def apply(timestamp: Long, interval: Interval): TimeWindow = {
    val intervalTimeInMs = interval.timeInSeconds
    val windowStart = (timestamp / intervalTimeInMs) * intervalTimeInMs
    val windowEnd = windowStart + intervalTimeInMs
    TimeWindow(windowStart, windowEnd)
  }
}

object Interval extends Enumeration {
  type Interval = IntervalVal

  def all: List[Interval] = {
    List(ONE_MINUTE, FIVE_MINUTE, FIFTEEN_MINUTE, ONE_HOUR)
  }

  sealed case class IntervalVal(name: String, timeInSeconds: Long) extends Val(name) {
  }

  val ONE_MINUTE = IntervalVal("OneMinute", 60)
  val FIVE_MINUTE = IntervalVal("FiveMinute", 300)
  val FIFTEEN_MINUTE = IntervalVal("FifteenMinute", 1500)
  val ONE_HOUR = IntervalVal("OneHour", 6000)
}
