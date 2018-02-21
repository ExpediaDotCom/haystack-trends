/*
 *  Copyright 2017 Expedia, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.expedia.www.haystack.trends.commons.entities

import com.expedia.www.haystack.trends.commons.entities.MetricType.MetricType

/**
  * The metricpoint object adheres to the metrics 2.0 specifications
  *
  * @param metric             : name of the metric
  * @param `type`             : type of the metric - see metric types below
  * @param tags               : key:value tags which add dimensions to the metric : eg : host, service_name etc
  * @param value              : value of the metric
  * @param epochTimeInSeconds : epochTime in seconds for when the event is generated
  */
case class MetricPoint(metric: String, `type`: MetricType, tags: Map[String, String], value: Float, epochTimeInSeconds: Long) {

  def getMetricPointKey(enablePeriodReplacement: Boolean): String = {
   val metricTags =  if (enablePeriodReplacement) {
      tags.foldLeft("")((tag, tuple) => {
        tag + s"${tuple._1}.${tuple._2.replace(".", "___")}."
      })
    } else {
      tags.foldLeft("")((tag, tuple) => {
        tag + s"${tuple._1}.${tuple._2}."
      })
    }
    s"haystack.$metricTags$metric"
  }
}

/*
The Metric types are according to metrics 2.0 specifications see http://metrics20.org/spec/#tag-keys
 */
object MetricType extends Enumeration {
  type MetricType = Value
  val Gauge = Value("gauge")
  val Count = Value("count")
  val Rate = Value("rate")
}


/*
The Tag keys are according to metrics 2.0 specifications see http://metrics20.org/spec/#tag-keys
 */
object TagKeys {
  val OPERATION_NAME_KEY = "operationName"
  val SERVICE_NAME_KEY = "serviceName"
  val RESULT_KEY = "result"
  val STATS_KEY = "stat"
  val ERROR_KEY = "error"
  val INTERVAL_KEY = "interval"
}



