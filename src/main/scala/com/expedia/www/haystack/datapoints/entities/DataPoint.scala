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
package com.expedia.www.haystack.datapoints.entities

import com.expedia.www.haystack.datapoints.entities.MetricType.MetricType

object DataPoint {

}

case class DataPoint(metric: String,
                     `type`: MetricType,
                     tags: Map[String, String],
                     value: Long,
                     timestamp: Long) {

  def getDataPointKey: String = {
    tags.foldLeft(s"$metric-")((tag, tuple) => {
      tag + s"${tuple._1}->${tuple._2}|"
    })
  }
}


object MetricType extends Enumeration {
  type MetricType = Value
  val Metric, Histogram, Aggregate = Value

}


object TagKeys {
  val OPERATION_NAME_KEY = "operationName"
  val SERVICE_NAME_KEY = "serviceName"
}



