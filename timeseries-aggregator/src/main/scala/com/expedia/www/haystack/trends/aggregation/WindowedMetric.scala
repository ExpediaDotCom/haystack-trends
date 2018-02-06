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

import com.codahale.metrics.{Meter, Timer}
import com.expedia.www.haystack.trends.aggregation.WindowedMetric._
import com.expedia.www.haystack.trends.aggregation.metrics.{Metric, MetricFactory}
import com.expedia.www.haystack.trends.commons.entities.Interval.Interval
import com.expedia.www.haystack.trends.commons.entities.MetricPoint
import com.expedia.www.haystack.trends.commons.metrics.MetricsSupport
import com.expedia.www.haystack.trends.entities.TimeWindow
import org.slf4j.LoggerFactory

import scala.collection.mutable.Queue
import scala.util.Try

/**
  * This class contains a metric for each time window being computed for a single trend. The number of time windows at any moment is = no. of intervals
  * if incoming metric point lies within the timewindow the metric is updated
  *
  * @param windowedMetrics :  data structure that contains interval, timewindows and metrics
  * @param metricFactory   :  factory which is used to create new metrics when required
  */
class WindowedMetric private(var windowedMetrics: Map[Interval, Queue[(TimeWindow, Metric)]], metricFactory: MetricFactory) extends MetricsSupport {

  private val disorderedMetricPointMeter: Meter = metricRegistry.meter("metricpoints.disordered")
  private val metricPointComputeFailureMeter: Meter = metricRegistry.meter("metricpoints.compute.failure")
  private val windowedMetricComputeTimer: Timer = metricRegistry.timer("windowed.metric.compute.time")
  private val invalidMetricPointMeter: Meter = metricRegistry.meter("metricpoints.invalid")

  val numberOfWatermarkedWindows = 1

  private var computedMetrics: List[(Long, Metric)] = List[(Long, Metric)]()


  def getMetricFactory: MetricFactory = {
    metricFactory
  }

  /**
    * function to compute the incoming metric point
    * it updates all the metrics for the windows within which the incoming metric point lies.
    *
    * @param incomingMetricPoint - incoming metric point
    */
  def compute(incomingMetricPoint: MetricPoint): Unit = {
    val timerContext = windowedMetricComputeTimer.time()
    Try {

      //discarding values which are less than 0 assuming they are invalid metric points
      if (incomingMetricPoint.value > 0) {
        windowedMetrics.foreach(windowedMetrics => {
          val currentInterval = windowedMetrics._1
          val currentIntervalQueue = windowedMetrics._2


          compareAndAddMetric(currentIntervalQueue: Queue[(TimeWindow, Metric)], currentInterval: Interval, incomingMetricPoint)

        })
      } else {
        invalidMetricPointMeter.mark()
      }
    }.recover {
      case failure: Throwable =>
        metricPointComputeFailureMeter.mark()
        LOGGER.error(s"Failed to compute metricpoint : $incomingMetricPoint with exception ", failure)
        failure
    }
    timerContext.close()
  }

  private def compareAndAddMetric(currentIntervalQueue: Queue[(TimeWindow, Metric)],
                                  currentInterval: Interval,
                                  incomingMetricPoint: MetricPoint) = {
    currentIntervalQueue.foreach(metricTuple => {
      val incomingMetricPointTimeWindow = TimeWindow.apply(incomingMetricPoint.epochTimeInSeconds, currentInterval)
      val currentTimeWindow = metricTuple._1
      val currentMetric = metricTuple._2

      currentTimeWindow.compare(incomingMetricPointTimeWindow) match {

        // compute to existing metric since in current window
        case number if number == 0 =>
          currentMetric.compute(incomingMetricPoint)

        // belongs to next window, lets flush this one to computedMetrics and create a new window
        case number if number < 0 =>
          val newMetric = metricFactory.createMetric(currentMetric.getMetricInterval)
          //newMetric.compute(incomingMetricPoint)    // not required since we will traverse over it after enqueue
          currentIntervalQueue.enqueue((incomingMetricPointTimeWindow, newMetric))
          evictMetric(currentIntervalQueue, currentInterval)

        // disordered metric point
        case _ =>
          // supported number of old windows already closed
          if (!currentIntervalQueue.map(metricTuple => metricTuple._1).toSet.contains(incomingMetricPointTimeWindow)) {
            disorderedMetricPointMeter.mark()
          }
      }
    })
  }

  def getComputedMetricPoints: List[MetricPoint] = {
    val metricPoints = computedMetrics.flatMap {
      case (publishTime, metric) =>
        metric.mapToMetricPoints(publishTime)
    }
    computedMetrics = List[(Long, Metric)]()
    metricPoints
  }

  def evictMetric(currentIntervalQueue: Queue[(TimeWindow, Metric)], interval: Interval) = {
    if (currentIntervalQueue.size > (numberOfWatermarkedWindows + 1)) {
      val evictedMetricTuple = currentIntervalQueue.dequeue()
      computedMetrics = (evictedMetricTuple._1.endTime, evictedMetricTuple._2) :: computedMetrics
    }
  }
}

  /**
    * Windowed metric factory which can create a new windowed metric or restore an existing windowed metric
    */
  object WindowedMetric {

    private val LOGGER = LoggerFactory.getLogger(this.getClass)

    def createWindowedMetric(intervals: List[Interval], firstMetricPoint: MetricPoint, metricFactory: MetricFactory): WindowedMetric = {
      val windowedMetrics = createMetricsForEachInterval(intervals, firstMetricPoint, metricFactory)
      new WindowedMetric(windowedMetrics, metricFactory)
    }

    def restoreMetric(windowedMetrics: Map[Interval, Queue[(TimeWindow, Metric)]], metricFactory: MetricFactory): WindowedMetric = {
      new WindowedMetric(windowedMetrics, metricFactory)
    }

    private def createMetricsForEachInterval(intervals: List[Interval],
                                             metricPoint: MetricPoint,
                                             metricFactory: MetricFactory): Map[Interval, Queue[(TimeWindow, Metric)]] = {

      intervals.map(interval => {
        val metrics = new Queue[(TimeWindow, Metric)]
        metrics.enqueue((TimeWindow.apply(metricPoint.epochTimeInSeconds, interval), metricFactory.createMetric(interval)))
        interval -> metrics
      }).toMap
    }
  }
