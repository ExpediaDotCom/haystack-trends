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

package com.expedia.www.haystack.metricpoints.aggregation

import com.codahale.metrics.Meter
import com.expedia.www.haystack.metricpoints.aggregation.metrics.{Metric, MetricFactory}
import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.entities.MetricType.MetricType
import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, TimeWindow}
import com.expedia.www.haystack.metricpoints.metrics.MetricsSupport

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class WindowedMetric(private val metricType: MetricType, private val intervals: List[Interval], firstMetricPoint: MetricPoint) extends MetricsSupport {


  val disorderedMetricPoints: Meter = metricRegistry.meter("disordered-metricpoints")

  //Todo: Have to add support for watermarking
  val numberOfWatermarkedWindows = 1

  val computedMetrics: ListBuffer[Metric] = mutable.ListBuffer[Metric]()

  val windowedMetricsMap: mutable.Map[TimeWindow, Metric] = createWindowedMetrics(firstMetricPoint)

  compute(firstMetricPoint)


  def compute(incomingMetricPoint: MetricPoint): Unit = {
    windowedMetricsMap.foreach(metricTimeWindowTuple => {
      val currentTimeWindow = metricTimeWindowTuple._1
      val currentMetric = metricTimeWindowTuple._2

      val incomingMetricPointTimeWindow = TimeWindow.apply(incomingMetricPoint.timestamp, currentMetric.getMetricInterval)

      compareAndAddMetric(currentTimeWindow, currentMetric, incomingMetricPointTimeWindow, incomingMetricPoint)
    })
  }

  def compareAndAddMetric(currentTimeWindow: TimeWindow, currentMetric: Metric, incomingMetricPointTimeWindow: TimeWindow, incomingMetricPoint: MetricPoint) = {

    currentTimeWindow.compare(incomingMetricPointTimeWindow) match {

      // compute to existing metric since in current window
      case number if number == 0 => currentMetric.compute(incomingMetricPoint)

      // belongs to next window, lets flush this one to computedMetrics and create a new window
      case number if number < 0 =>
        windowedMetricsMap.remove(currentTimeWindow)
        windowedMetricsMap.put(incomingMetricPointTimeWindow, MetricFactory.getMetric(metricType, currentMetric.getMetricInterval).get)
        computedMetrics += currentMetric

      // window already closed and we don't support water marking yet
      case _ => disorderedMetricPoints.mark()
    }
  }

  def getComputedMetricPoints: List[MetricPoint] = {
    val metricPoint = computedMetrics.toList.flatMap(metric => {
      metric.mapToMetricPoints()
    })
    computedMetrics.clear()
    metricPoint
  }


  private def createWindowedMetrics(metricPoint: MetricPoint): mutable.Map[TimeWindow, Metric] = {
    val metrics = mutable.Map[TimeWindow, Metric]()
    intervals.foreach(interval => {
      metrics.put(TimeWindow.apply(metricPoint.timestamp, interval), MetricFactory.getMetric(metricType, interval).get)
    })
    metrics
  }


}
