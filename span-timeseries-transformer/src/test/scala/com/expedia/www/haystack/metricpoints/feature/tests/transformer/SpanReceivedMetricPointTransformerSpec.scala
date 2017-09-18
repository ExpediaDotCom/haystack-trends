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
package com.expedia.www.haystack.metricpoints.feature.tests.transformer

import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.metricpoints.entities.TagKeys
import com.expedia.www.haystack.metricpoints.feature.FeatureSpec
import com.expedia.www.haystack.metricpoints.transformer.SpanReceivedMetricPointTransformer

class SpanReceivedMetricPointTransformerSpec extends FeatureSpec with SpanReceivedMetricPointTransformer {

  feature("metricPoint transformer for creating total count metricPoint") {
    scenario("should have a total-count metricPoint given span which is successful") {

      Given("a successful span object")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val span = Span.newBuilder()
        .setDuration(duration)
        .setOperationName(operationName)
        .setServiceName(serviceName)
        .addTags(Tag.newBuilder().setKey(TagKeys.ERROR_KEY).setVBool(false))
        .build()

      When("metricPoint is created using transformer")
      val metricPoints = mapSpan(span)

      Then("should only have 1 metricPoint")
      metricPoints.length shouldEqual 1

      Then("the metricPoint value should be 1")
      metricPoints.head.value shouldEqual 1

      Then("metric name should be total-count")
      metricPoints.head.metric shouldEqual TOTAL_METRIC_NAME
    }
    scenario("should have a total-count metricPoint given span which is erroneous") {

      Given("an erroneous span object")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val span = Span.newBuilder()
        .setDuration(duration)
        .setOperationName(operationName)
        .setServiceName(serviceName)
        .addTags(Tag.newBuilder().setKey(TagKeys.ERROR_KEY).setVBool(true))
        .build()

      When("metricPoint is created using transformer")
      val metricPoints = mapSpan(span)

      Then("should only have 1 metricPoint")
      metricPoints.length shouldEqual 1

      Then("the metricPoint value should be 1")
      metricPoints.head.value shouldEqual 1

      Then("metric name should be total-count")
      metricPoints.head.metric shouldEqual TOTAL_METRIC_NAME
    }
  }
}
