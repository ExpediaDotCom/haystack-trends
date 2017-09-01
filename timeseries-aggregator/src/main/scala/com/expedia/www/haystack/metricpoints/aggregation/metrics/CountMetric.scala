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

import com.expedia.www.haystack.metricpoints.aggregation.metrics.CountMetric._
import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.entities.StatValue.StatValue
import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, MetricType, StatValue, TagKeys}

object CountMetric {
  def appendTags(metricPoint: MetricPoint, interval: Interval, statValue: StatValue): Map[String, String] = {
    metricPoint.tags + (TagKeys.INTERVAL_KEY -> interval.name, TagKeys.STATS_KEY -> statValue.toString)
  }
}

class CountMetric(interval: Interval) extends Metric(interval) {

  var latestMetricPoint: Option[MetricPoint] = None
  var currentCount: Long = 0

  override def mapToMetricPoints(windowEndTimestamp: Long = latestMetricPoint.map(_.epochTimeInSeconds).getOrElse(System.currentTimeMillis())): List[MetricPoint] = {
    latestMetricPoint.map { metricPoint =>
      List(
        MetricPoint(metricPoint.metric, MetricType.Count, appendTags(metricPoint, interval, StatValue.COUNT), currentCount, windowEndTimestamp)
      )
    }.getOrElse(List())
  }

  override def compute(metricPoint: MetricPoint): CountMetric = {
    currentCount += metricPoint.value.toLong
    latestMetricPoint = Some(metricPoint)
    this
  }
}
