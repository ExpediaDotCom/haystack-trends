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

import com.expedia.www.haystack.commons.entities.Interval.Interval
import com.expedia.www.haystack.commons.entities.{MetricPoint, TagKeys}
import com.expedia.www.haystack.commons.metrics.MetricsSupport
import com.expedia.www.haystack.trends.aggregation.metrics.AggregationType.AggregationType
import com.expedia.www.haystack.trends.aggregation.entities.StatValue.StatValue
import com.expedia.www.haystack.trends.kstream.serde.metric.MetricSerde

abstract class Metric(interval: Interval) extends MetricsSupport {

  /**
    * function to compute the incoming metric-point
    *
    * @param value - incoming metric point
    * @return : returns the metric (in most cases it should return the same object(this) but returning a metric gives the metric implementation class to create an immutable metric)
    */
  def compute(value: MetricPoint): Metric

  def getMetricInterval: Interval = {
    interval
  }


  /**
    * This function returns the metric points which contains the current snapshot of the metric
    *
    * @param publishingTimestamp : timestamp in seconds which the consumer wants to be used as the timestamps of these published metricpoints
    * @param metricName : the name of the metricpoints to be generated
    * @param tags : tags to be associated with the metricPoints
    * @return list of published metricpoints
    */
  def mapToMetricPoints(metricName: String, tags: Map[String, String], publishingTimestamp: Long): List[MetricPoint]

  protected def appendTags(tags: Map[String, String], interval: Interval, statValue: StatValue): Map[String, String] = {
    tags + (TagKeys.INTERVAL_KEY -> interval.name, TagKeys.STATS_KEY -> statValue.toString)
  }

}

/**
  * The enum contains the support aggregation type, which is currently count and histogram
  */
object AggregationType extends Enumeration {
  type AggregationType = Value
  val Count, Histogram = Value
}


/**
  * This trait is supposed to be created by every metric class which lets them create the metric when ever required
  */
trait MetricFactory {
  def createMetric(interval: Interval): Metric

  def getAggregationType: AggregationType

  def getMetricSerde: MetricSerde
}
