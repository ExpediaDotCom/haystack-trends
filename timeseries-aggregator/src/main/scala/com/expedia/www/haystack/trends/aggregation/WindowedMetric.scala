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

package com.expedia.www.haystack.trends.aggregation

import com.codahale.metrics.Meter
import com.expedia.www.haystack.trends.aggregation.metrics.{Metric, MetricFactory}
import com.expedia.www.haystack.trends.commons.entities.Interval.Interval
import com.expedia.www.haystack.trends.commons.entities.MetricPoint
import com.expedia.www.haystack.trends.commons.metrics.MetricsSupport
import com.expedia.www.haystack.trends.entities.TimeWindow
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * This class contains a metric for each time window being computed for a single interval
  *
  * @param windowedMetricsMap map containing  sorted timewindows and metrics for an Interval
  * @param metricFactory      factory which is used to create new metrics when required
  */
class WindowedMetric private(var windowedMetricsMap: mutable.TreeMap[TimeWindow, Metric], metricFactory: MetricFactory, numberOfWatermarkedWindows: Int, interval: Interval) extends MetricsSupport {

  private val disorderedMetricPointMeter: Meter = metricRegistry.meter("metricpoints.disordered")
  private var computedMetrics = List[(Long, Metric)]()

  def getMetricFactory: MetricFactory = {
    metricFactory
  }

  /**
    * function to compute the incoming metric point
    * it updates all the metrics for the windows within which the incoming metric point lies for an interval
    *
    * @param incomingMetricPoint - incoming metric point
    */
  def compute(incomingMetricPoint: MetricPoint): Unit = {
    val incomingMetricPointTimeWindow = TimeWindow.apply(incomingMetricPoint.epochTimeInSeconds, interval)

    val matchedWindowedMetric = windowedMetricsMap.get(incomingMetricPointTimeWindow)

    if (matchedWindowedMetric.isDefined) {
      // an existing metric
      matchedWindowedMetric.get.compute(incomingMetricPoint)
    } else {
      // incoming metric is a new metric
      if (incomingMetricPointTimeWindow.compare(windowedMetricsMap.firstKey) > 0) {
        // incoming metric's time is more that minimum (first) time window
        createNewMetric(incomingMetricPointTimeWindow, incomingMetricPoint)
        evictMetric()
      } else {
        // disordered metric
        disorderedMetricPointMeter.mark()
      }
    }
  }

  private def createNewMetric(incomingMetricPointTimeWindow: TimeWindow, incomingMetricPoint: MetricPoint) = {
    val newMetric = metricFactory.createMetric(interval)
    newMetric.compute(incomingMetricPoint)
    windowedMetricsMap.put(incomingMetricPointTimeWindow, newMetric)
  }

  private def evictMetric() = {
    if (windowedMetricsMap.size > (numberOfWatermarkedWindows + 1)) {
      val evictInterval = windowedMetricsMap.firstKey
      windowedMetricsMap.remove(evictInterval).foreach { evictedMetric =>
        computedMetrics = (evictInterval.endTime, evictedMetric) :: computedMetrics
      }
    }
  }

  /**
    * returns list of metricPoints which are evicted and their window is closes
    *
    * @return list of evicted metricPoints
    */
  def getComputedMetricPoints(incomingMetricPoint: MetricPoint): List[MetricPoint] = {
    val metricPoints = computedMetrics.flatMap {
      case (publishTime, metric) =>
        metric.mapToMetricPoints(incomingMetricPoint.metric, incomingMetricPoint.tags, publishTime)
    }
    computedMetrics = List[(Long, Metric)]()
    metricPoints
  }
}

/**
  * Windowed metric factory which can create a new windowed metric or restore an existing windowed metric for an interval
  */
object WindowedMetric {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)

  def createWindowedMetric(firstMetricPoint: MetricPoint, metricFactory: MetricFactory, watermarkedWindows: Int, interval: Interval): WindowedMetric = {
    val windowedMetricMap = mutable.TreeMap[TimeWindow, Metric]()
    val metric = metricFactory.createMetric(interval)
    metric.compute(firstMetricPoint)
    windowedMetricMap.put(TimeWindow.apply(firstMetricPoint.epochTimeInSeconds, interval), metric)
    new WindowedMetric(windowedMetricMap, metricFactory, watermarkedWindows, interval)
  }

  def restoreWindowedMetric(windowedMetricsMap: mutable.TreeMap[TimeWindow, Metric], metricFactory: MetricFactory, watermarkedWindows: Int, interval: Interval): WindowedMetric = {
    new WindowedMetric(windowedMetricsMap, metricFactory, watermarkedWindows, interval)
  }
}
