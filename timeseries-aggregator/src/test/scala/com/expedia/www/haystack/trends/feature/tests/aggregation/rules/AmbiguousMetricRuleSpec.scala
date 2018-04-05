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

package com.expedia.www.haystack.trends.feature.tests.aggregation.rules

import com.expedia.www.haystack.commons.entities.{MetricPoint, MetricType}
import com.expedia.www.haystack.trends.aggregation.metrics.AggregationType
import com.expedia.www.haystack.trends.aggregation.rules.AmbiguousMetricRule
import com.expedia.www.haystack.trends.feature.FeatureSpec

class AmbiguousMetricRuleSpec extends FeatureSpec with AmbiguousMetricRule {

  val AMBIGUOUS_METRIC_NAME = "ambiguous-spans"
  val SERVICE_NAME = "dummy_service"
  val OPERATION_NAME = "dummy_operation"

  object TagKeys {
    val OPERATION_NAME_KEY = "operationName"
    val SERVICE_NAME_KEY = "serviceName"
  }

  feature("AmbiguousMetricRule for identifying MetricRule") {


    scenario("should get Aggregate AggregationType for an Ambiguous MetricPoint") {

      Given("an ambiguous MetricPoint")
      val keys = Map(TagKeys.OPERATION_NAME_KEY -> OPERATION_NAME,
        TagKeys.SERVICE_NAME_KEY -> SERVICE_NAME)
      val startTime = currentTimeInSecs

      val metricPoint = MetricPoint(AMBIGUOUS_METRIC_NAME, MetricType.Gauge, keys, 1, startTime)

      When("trying to find matching AggregationType")
      val aggregationType = isMatched(metricPoint)

      Then("should get Aggregate AggregationType")
      aggregationType shouldEqual Some(AggregationType.Count)
    }

    scenario("should NOT Aggregate AggregationType for non-Ambiguous MetricPoints") {

      Given("a non-ambiguous MetricPoint")
      val keys = Map(TagKeys.OPERATION_NAME_KEY -> OPERATION_NAME,
        TagKeys.SERVICE_NAME_KEY -> SERVICE_NAME)
      val startTime = currentTimeInSecs

      val metricPoint = MetricPoint("non-ambiguous", MetricType.Gauge, keys, 1, startTime)

      When("trying to find matching AggregationType")
      val aggregationType = isMatched(metricPoint)

      Then("should not get Aggregate AggregationType")
      aggregationType shouldEqual None
    }
  }
}
