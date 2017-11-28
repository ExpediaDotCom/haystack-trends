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

package com.expedia.www.haystack.trends.commons.unit.tests

import com.expedia.www.haystack.trends.commons.entities.{MetricPoint, MetricType, TagKeys}
import com.expedia.www.haystack.trends.commons.unit.UnitTestSpec


class MetricPointSpec extends UnitTestSpec {

  val DURATION_METRIC_NAME = "duration"
  val SERVICE_NAME = "dummy.service_name"
  val OPERATION_NAME = "dummy.operation.name"
  val keys = Map(TagKeys.OPERATION_NAME_KEY -> OPERATION_NAME,
    TagKeys.SERVICE_NAME_KEY -> SERVICE_NAME)

  "MetricPoint entity" should {

    "append tags with underscore" in {

      Given("metric point")
      val metricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, keys, 80, currentTimeInSecs)

      When("its serialized using the metricTank Serde")
      val metricPointKey = metricPoint.getMetricPointKey

      Then("it should be encoded as message pack")
      metricPointKey shouldEqual
        TagKeys.OPERATION_NAME_KEY + ":" + OPERATION_NAME.replace(".", "_") + "." +
          TagKeys.SERVICE_NAME_KEY + ":" + SERVICE_NAME.replace(".", "_") + "." +
          DURATION_METRIC_NAME
    }
  }
}
