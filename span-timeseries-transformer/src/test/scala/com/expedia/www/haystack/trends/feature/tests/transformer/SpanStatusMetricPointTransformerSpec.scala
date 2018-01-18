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
import com.expedia.www.haystack.trends.commons.entities.TagKeys
import com.expedia.www.haystack.trends.feature.FeatureSpec
import com.expedia.www.haystack.trends.transformer.SpanStatusMetricPointTransformer

class SpanStatusMetricPointTransformerSpec extends FeatureSpec with SpanStatusMetricPointTransformer {

  feature("metricPoint transformer for creating status count metricPoint") {

    scenario("should have a success-spans metricPoint given span which is successful " +
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
      val metricPointKey = TagKeys.SERVICE_NAME_KEY + "." + span.getServiceName + "." +
        TagKeys.OPERATION_NAME_KEY + "." + span.getOperationName + "." +
        SUCCESS_METRIC_NAME
      val metricPointServiceOnlyKey = TagKeys.SERVICE_NAME_KEY + "." + span.getServiceName + "." +
        SUCCESS_METRIC_NAME

      When("metricPoint is created using the transformer")
      val metricPoints = mapSpan(span, true)

      Then("should only have 2 metricPoint")
      metricPoints.length shouldEqual 2

      Then("the metricPoint value should be 1")
      metricPoints(0).value shouldEqual 1
      metricPoints(1).value shouldEqual 1

      Then("metric name should be success-spans")
      metricPoints.head.metric shouldEqual SUCCESS_METRIC_NAME

      Then("returned keys should be as expected")
      val metricPointKeys = metricPoints.map(metricPoint => metricPoint.getMetricPointKey(true)).toSet
      metricPointKeys shouldBe (Set(metricPointKey, metricPointServiceOnlyKey))
    }

    scenario("should have a failure-spans metricPoint given span  which is erroneous " +
      "and when service level generation is enabled") {

      Given("a erroneous span object")
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

      Then("metric name should be failure-spans")
      metricPoints.head.metric shouldEqual FAILURE_METRIC_NAME
    }

    scenario("should return an empty list when error key is missing in span tags " +
      "and when service level generation is enabled") {

      Given("a span object which missing error tag")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val span = Span.newBuilder()
        .setDuration(duration)
        .setOperationName(operationName)
        .setServiceName(serviceName)
        .build()

      When("metricPoint is created using transformer")
      val metricPoints = mapSpan(span, true)

      Then("should not return metricPoints")
      metricPoints.length shouldEqual 0
    }

    scenario("should have a success-spans metricPoint given span which is successful " +
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
      val metricPointKey = TagKeys.SERVICE_NAME_KEY + "." + span.getServiceName + "." +
        TagKeys.OPERATION_NAME_KEY + "." + span.getOperationName + "." +
        SUCCESS_METRIC_NAME

      When("metricPoint is created using the transformer")
      val metricPoints = mapSpan(span, false)

      Then("should only have 1 metricPoint")
      metricPoints.length shouldEqual 1

      Then("the metricPoint value should be 1")
      metricPoints(0).value shouldEqual 1

      Then("metric name should be success-spans")
      metricPoints.head.metric shouldEqual SUCCESS_METRIC_NAME

      Then("returned keys should be as expected")
      metricPoints.head.getMetricPointKey(true) shouldBe (metricPointKey)
    }

    scenario("should have a failure-spans metricPoint given span  which is erroneous " +
      "and when service level generation is disabled") {

      Given("a erroneous span object")
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

      Then("metric name should be failure-spans")
      metricPoints.head.metric shouldEqual FAILURE_METRIC_NAME
    }

    scenario("should return an empty list when error key is missing in span tags " +
      "and when service level generation is disabled") {

      Given("a span object which missing error tag")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val span = Span.newBuilder()
        .setDuration(duration)
        .setOperationName(operationName)
        .setServiceName(serviceName)
        .build()

      When("metricPoint is created using transformer")
      val metricPoints = mapSpan(span, false)

      Then("should not return metricPoints")
      metricPoints.length shouldEqual 0
    }
  }
}
