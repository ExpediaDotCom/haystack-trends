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

import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.commons.entities.TagKeys
import com.expedia.www.haystack.trends.feature.FeatureSpec
import com.expedia.www.haystack.trends.transformer.SpanReceivedMetricPointTransformer

class SpanReceivedMetricPointTransformerSpec extends FeatureSpec with SpanReceivedMetricPointTransformer {

  feature("metricPoint transformer for creating total count metricPoint") {
    scenario("should have a total-count metricPoint given span which is successful " +
      "and when service level generation is enabled") {

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
      val metricPointKey = "haystack."+TagKeys.SERVICE_NAME_KEY + "." + span.getServiceName + "." +
        TagKeys.OPERATION_NAME_KEY + "." + span.getOperationName + "." +
        TOTAL_METRIC_NAME
      val metricPointServiceOnlyKey = "haystack."+TagKeys.SERVICE_NAME_KEY + "." + span.getServiceName + "." +
        TOTAL_METRIC_NAME

      When("metricPoint is created using transformer")
      val metricPoints = mapSpan(span, true)

      Then("should only have 2 metricPoint")
      metricPoints.length shouldEqual 2

      Then("the metricPoint value should be 1")
      metricPoints.head.value shouldEqual 1
      metricPoints(1).value shouldEqual 1

      Then("metric name should be total-count")
      metricPoints.head.metric shouldEqual TOTAL_METRIC_NAME

      Then("returned keys should be as expected")
      val metricPointKeys = metricPoints.map(metricPoint => metricPoint.getMetricPointKey(true, false)).toSet
      metricPointKeys shouldBe Set(metricPointKey, metricPointServiceOnlyKey)
    }

    scenario("should have a total-count metricPoint given span which is erroneous " +
      "and when service level generation is enabled") {

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
      val metricPoints = mapSpan(span, true)

      Then("should only have 2 metricPoint")
      metricPoints.length shouldEqual 2

      Then("the metricPoint value should be 1")
      metricPoints(0).value shouldEqual 1
      metricPoints(1).value shouldEqual 1

      Then("metric name should be total-count")
      metricPoints.head.metric shouldEqual TOTAL_METRIC_NAME

    }
  }

  scenario("should have a total-count metricPoint given span which is successful " +
    "and when service level generation is disabled") {

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
    val metricPointKey = "haystack."+TagKeys.SERVICE_NAME_KEY + "." + span.getServiceName + "." +
      TagKeys.OPERATION_NAME_KEY + "." + span.getOperationName + "." +
      TOTAL_METRIC_NAME

    When("metricPoint is created using transformer")
    val metricPoints = mapSpan(span, false)

    Then("should only have 1 metricPoint")
    metricPoints.length shouldEqual 1

    Then("the metricPoint value should be 1")
    metricPoints(0).value shouldEqual 1

    Then("metric name should be total-count")
    metricPoints.head.metric shouldEqual TOTAL_METRIC_NAME

    Then("returned keys should be as expected")
    metricPoints.head.getMetricPointKey(true, false) shouldBe metricPointKey
  }

  scenario("should have a total-count metricPoint given span which is erroneous " +
    "and when service level generation is disabled") {

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
    val metricPoints = mapSpan(span, false)

    Then("should only have 1 metricPoint")
    metricPoints.length shouldEqual 1

    Then("the metricPoint value should be 1")
    metricPoints(0).value shouldEqual 1

    Then("metric name should be total-count")
    metricPoints.head.metric shouldEqual TOTAL_METRIC_NAME
  }
}
