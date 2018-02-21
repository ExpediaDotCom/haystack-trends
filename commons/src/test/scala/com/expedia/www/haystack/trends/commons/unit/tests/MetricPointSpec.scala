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
  val SERVICE_NAME_WITH_DOT = "dummy.service.name"
  val SERVICE_NAME_WITH_COLON = "dummy:service-name"
  val OPERATION_NAME_WITH_DOT = "dummy.operation.name"
  val OPERATION_NAME_WITH_COLON = "dummy:operation-name"

  "MetricPoint entity" should {

    "replace period with underscore in tag values for metric point key" in {

      Given("metric point with period in service and operation name")
      val keys = Map(TagKeys.OPERATION_NAME_KEY -> OPERATION_NAME_WITH_DOT,
        TagKeys.SERVICE_NAME_KEY -> SERVICE_NAME_WITH_DOT)
      val metricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, keys, 80, currentTimeInSecs)

      When("we get the metric point key with config enabled")
      val metricPointKey = metricPoint.getMetricPointKey(true)

      Then("metric point key should have value with period replaced with underscore")
      metricPointKey shouldEqual
        "haystack."+TagKeys.OPERATION_NAME_KEY + "." + OPERATION_NAME_WITH_DOT.replace(".", "___") + "." +
          TagKeys.SERVICE_NAME_KEY + "." + SERVICE_NAME_WITH_DOT.replace(".", "___") + "." +
          DURATION_METRIC_NAME
    }

    "should not replace period with underscore in tag values for metric point key" in {

      Given("metric point with period in service and operation name")
      val keys = Map(TagKeys.OPERATION_NAME_KEY -> OPERATION_NAME_WITH_DOT,
        TagKeys.SERVICE_NAME_KEY -> SERVICE_NAME_WITH_DOT)
      val metricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, keys, 80, currentTimeInSecs)

      When("we get the metric point key with config disabled")
      val metricPointKey = metricPoint.getMetricPointKey(false)

      Then("metric point key should have value with period replaced with underscore")
      metricPointKey shouldEqual
        "haystack."+TagKeys.OPERATION_NAME_KEY + "." + OPERATION_NAME_WITH_DOT + "." +
          TagKeys.SERVICE_NAME_KEY + "." + SERVICE_NAME_WITH_DOT + "." +
          DURATION_METRIC_NAME
    }

    "not do anything to colon in operation name" in {

      Given("metric point with operation name consisting of colon")
      val keys = Map(TagKeys.OPERATION_NAME_KEY -> OPERATION_NAME_WITH_COLON,
        TagKeys.SERVICE_NAME_KEY -> SERVICE_NAME_WITH_COLON)
      val metricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, keys, 80, currentTimeInSecs)

      When("we get the metric point key")
      val metricPointKey = metricPoint.getMetricPointKey(true)

      Then("metric point key should have value with only period replaced with underscore and colon retained")
      metricPointKey shouldEqual
        "haystack."+TagKeys.OPERATION_NAME_KEY + "." + OPERATION_NAME_WITH_COLON.replace(".", "___") + "." +
          TagKeys.SERVICE_NAME_KEY + "." + SERVICE_NAME_WITH_COLON.replace(".", "___") + "." +
          DURATION_METRIC_NAME
    }
  }
}
