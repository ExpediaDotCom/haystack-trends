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

package com.expedia.www.haystack.metricpoints.aggregation.metrics

import com.expedia.www.haystack.metricpoints.aggregation.metrics.AggregationType.AggregationType
import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, MetricType, StatValue}
import com.expedia.www.haystack.metricpoints.kstream.serde.metric.{CountMetricSerde, MetricSerde}


class CountMetric(interval: Interval, var currentCount: Long) extends Metric(interval) {

  def this(interval: Interval) = this(interval, 0)

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
    currentCount += metricPoint.value.toLong
    latestMetricPoint = Some(metricPoint)
    this
  }
}

object CountMetricFactory extends MetricFactory {
  override def createMetric(interval: Interval): CountMetric = new CountMetric(interval)

  override def getAggregationType: AggregationType = AggregationType.Count

  override def getMetricSerde: MetricSerde = CountMetricSerde
}
