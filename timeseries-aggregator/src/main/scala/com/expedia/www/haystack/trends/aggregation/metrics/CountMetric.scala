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

package com.expedia.www.haystack.trends.aggregation.metrics

import com.codahale.metrics.Timer
import com.expedia.www.haystack.trends.aggregation.metrics.AggregationType.AggregationType
import com.expedia.www.haystack.trends.commons.entities.Interval.Interval
import com.expedia.www.haystack.trends.commons.entities.{MetricPoint, MetricType}
import com.expedia.www.haystack.trends.entities.StatValue
import com.expedia.www.haystack.trends.kstream.serde.metric.{CountMetricSerde, MetricSerde}

/**
  * This is a base metric which can compute the count of the given events
  *
  * @param interval     : interval for the metric
  * @param currentCount : current count, the current count should be 0 for a new metric but can be passed when we want to restore a given metric after the application crashed
  */

class CountMetric(interval: Interval, var currentCount: Long) extends Metric(interval) {

  def this(interval: Interval) = this(interval, 0)

  private val CountMetricComputeTimer: Timer = metricRegistry.timer("count.metric.compute.time")

  var latestMetricPoint: Option[MetricPoint] = None

  override def mapToMetricPoints(publishingTimestamp: Long): List[MetricPoint] = {
    latestMetricPoint match {
      case Some(metricPoint) =>
        List(
          MetricPoint(metricPoint.metric, MetricType.Count, appendTags(metricPoint, interval, StatValue.COUNT), currentCount, publishingTimestamp)
        )
      case None => List()
    }
  }

  def getCurrentCount: Long = {
    currentCount
  }


  override def compute(metricPoint: MetricPoint): CountMetric = {
    val timerContext = CountMetricComputeTimer.time()
    currentCount += metricPoint.value.toLong
    latestMetricPoint = Some(metricPoint)
    timerContext.close()
    this
  }
}

object CountMetricFactory extends MetricFactory {
  override def createMetric(interval: Interval): CountMetric = new CountMetric(interval)

  override def getAggregationType: AggregationType = AggregationType.Count

  override def getMetricSerde: MetricSerde = CountMetricSerde
}
