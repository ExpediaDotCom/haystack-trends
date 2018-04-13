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
package com.expedia.www.haystack.trends.feature.tests.transformer

import com.expedia.www.haystack.commons.entities.TagKeys
import com.expedia.www.haystack.commons.entities.encoders.PeriodReplacementEncoder
import com.expedia.www.haystack.trends.feature.FeatureSpec
import com.expedia.www.haystack.trends.transformer.SpanDurationMetricPointTransformer

class SpanDurationMetricPointTransformerSpec extends FeatureSpec with SpanDurationMetricPointTransformer {

  feature("metricPoint transformer for creating duration metricPoint") {
    scenario("should have duration value in metricPoint for given duration in span " +
      "and when service level generation is enabled") {

      Given("a valid span object")
      val duration = System.currentTimeMillis
      val span = generateTestSpan(duration)
      val metricPointKey = "haystack."+TagKeys.SERVICE_NAME_KEY + "." + span.getServiceName + "." +
        TagKeys.OPERATION_NAME_KEY + "." + span.getOperationName + "." +
        DURATION_METRIC_NAME
      val metricPointServiceOnlyKey = "haystack."+TagKeys.SERVICE_NAME_KEY + "." + span.getServiceName + "." +
        DURATION_METRIC_NAME

      When("metricPoint is created using transformer")
      val metricPoints = mapSpan(span, true)

      Then("should only have 2 metricPoint")
      metricPoints.length shouldEqual 2

      Then("same duration should be in metricPoint value")
      metricPoints.head.value shouldEqual duration


      Then("the metric name should be duration")
      metricPoints.head.metric shouldEqual DURATION_METRIC_NAME

      Then("returned keys should be as expected")
      val metricPointKeys = metricPoints.map(metricPoint => metricPoint.getMetricPointKey(new PeriodReplacementEncoder)).toSet
      metricPointKeys shouldBe Set(metricPointKey, metricPointServiceOnlyKey)
    }

    scenario("should have duration value in metricPoint for given duration in span " +
      "and when service level generation is disabled") {

      Given("a valid span object")
      val duration = System.currentTimeMillis
      val span = generateTestSpan(duration)
      val metricPointKey = "haystack."+TagKeys.SERVICE_NAME_KEY + "." + span.getServiceName + "." +
        TagKeys.OPERATION_NAME_KEY + "." + span.getOperationName + "." +
        DURATION_METRIC_NAME

      When("metricPoint is created using transformer")
      val metricPoints = mapSpan(span, false)

      Then("should only have 1 metricPoint")
      metricPoints.length shouldEqual 1

      Then("same duration should be in metricPoint value")
      metricPoints.head.value shouldEqual duration


      Then("the metric name should be duration")
      metricPoints.head.metric shouldEqual DURATION_METRIC_NAME

      Then("returned keys should be as expected")
      metricPoints.head.getMetricPointKey(new PeriodReplacementEncoder) shouldBe metricPointKey
    }
  }
}
