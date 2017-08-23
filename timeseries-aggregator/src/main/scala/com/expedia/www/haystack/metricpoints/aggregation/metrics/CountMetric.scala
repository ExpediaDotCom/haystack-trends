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

import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, MetricType}

object CountMetric {
  def getMetricName(metricPoint: MetricPoint, interval: Interval): String = {
    s"${metricPoint.metric}.${interval.name}.count"
  }
}

class CountMetric(interval: Interval) extends Metric(interval) {

  var latestMetricPoint: Option[MetricPoint] = None
  var currentCount: Long = 0

  override def mapToMetricPoints(windowEndTimestamp: Long = latestMetricPoint.map(_.timestamp).getOrElse(System.currentTimeMillis())): List[MetricPoint] = {
    latestMetricPoint.map { metricPoint =>
      val metricName = CountMetric.getMetricName(metricPoint, interval)
      List(
        MetricPoint(metricName, MetricType.Aggregate, metricPoint.tags, currentCount, windowEndTimestamp)
      )
    }.getOrElse(List())
  }

  override def compute(metricPoint: MetricPoint): CountMetric = {
    currentCount += 1
    latestMetricPoint = Some(metricPoint)
    this
  }
}
