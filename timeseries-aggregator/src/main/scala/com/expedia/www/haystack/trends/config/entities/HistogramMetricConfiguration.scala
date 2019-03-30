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
package com.expedia.www.haystack.trends.config.entities


/**
  *This configuration helps create the HistogramMetric.
  * @param precision - Decimal precision required for the histogram,allowable precision of histogram must be 0 <= value <= 5
  * @param maxValue - maximum value for the incoming metric (should always be > than the maximum value you're expecting for a metricpoint)
  * @param unit - unit of the value that will be given to histogram (can be ms, micro, sec)
  */
case class HistogramMetricConfiguration(precision: Int, maxValue: Int, unit: HistogramUnit)

class HistogramUnit (unit: String) {
  def isMillis: Boolean = {
    unit.equalsIgnoreCase("ms")
  }

  def isMicros: Boolean = {
    unit.equalsIgnoreCase("micro")
  }

  def isSeconds: Boolean = {
    unit.equalsIgnoreCase("sec")
  }
}

