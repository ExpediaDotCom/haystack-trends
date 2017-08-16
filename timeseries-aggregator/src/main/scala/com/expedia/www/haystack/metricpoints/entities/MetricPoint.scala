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
package com.expedia.www.haystack.metricpoints.entities

import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.entities.MetricType.MetricType


case class MetricPoint(metric: String,
                       `type`: MetricType,
                       tags: Map[String, String],
                       value: Long,
                       timestamp: Long) {

  def getMetricPointKey: String = {
    tags.foldLeft(s"$metric-")((tag, tuple) => {
      tag + s"${tuple._1}->${tuple._2}|"
    })
  }
}

object MetricType extends Enumeration {
  type MetricType = Value
  val Metric, Histogram, Aggregate = Value

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