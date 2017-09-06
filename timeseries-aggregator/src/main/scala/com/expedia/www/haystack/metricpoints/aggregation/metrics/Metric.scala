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
import com.expedia.www.haystack.metricpoints.entities.StatValue.StatValue
import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, TagKeys}

abstract class Metric(interval: Interval) {

  def compute(value: MetricPoint): Metric

  def getMetricInterval: Interval = {
    interval
  }

  def mapToMetricPoints(publishingTimestamp: Long): List[MetricPoint]

  protected def appendTags(metricPoint: MetricPoint, interval: Interval, statValue: StatValue): Map[String, String] = {
    metricPoint.tags + (TagKeys.INTERVAL_KEY -> interval.name, TagKeys.STATS_KEY -> statValue.toString)
  }

}

object AggregationType extends Enumeration {
  type AggregationType = Value
  val Count, Histogram = Value
}


trait MetricFactory {
  def createMetric(interval: Interval): Metric = ???
}
