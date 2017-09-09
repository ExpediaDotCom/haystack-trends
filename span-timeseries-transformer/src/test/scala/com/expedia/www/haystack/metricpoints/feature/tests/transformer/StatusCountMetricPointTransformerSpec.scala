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

import com.expedia.open.tracing.{Process, Span, Tag}
import com.expedia.www.haystack.metricpoints.entities.TagKeys
import com.expedia.www.haystack.metricpoints.feature.FeatureSpec
import com.expedia.www.haystack.metricpoints.transformer.StatusCountMetricPointTransformer

class StatusCountMetricPointTransformerSpec extends FeatureSpec with StatusCountMetricPointTransformer {

  feature("metricPoint transformer for creating status count metricPoint") {

    scenario("should have a success-spans metricPoint given span which is successful") {

      Given("a successful span object")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val process = Process.newBuilder().setServiceName(serviceName)
      val span = Span.newBuilder()
        .setDuration(duration)
        .setOperationName(operationName)
        .setProcess(process)
        .addTags(Tag.newBuilder().setKey(TagKeys.ERROR_KEY).setVBool(false))
        .build()

      When("metricPoint is created using the transformer")
      val metricPoints = mapSpan(span)

      Then("should only have 1 metricPoint")
      metricPoints.length shouldEqual 1

      Then("the metricPoint value should be 1")
      metricPoints.head.value shouldEqual 1

      Then("metric name should be success-spans")
      metricPoints.head.metric shouldEqual SUCCESS_METRIC_NAME
    }

    scenario("should have a failure-spans metricPoint given span  which is erroneous") {

      Given("a erroneous span object")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val process = Process.newBuilder().setServiceName(serviceName)
      val span = Span.newBuilder()
        .setDuration(duration)
        .setOperationName(operationName)
        .setProcess(process)
        .addTags(Tag.newBuilder().setKey(TagKeys.ERROR_KEY).setVBool(true))
        .build()

      When("metricPoint is created using transformer")
      val metricPoints = mapSpan(span)

      Then("should only have 1 metricPoint")
      metricPoints.length shouldEqual 1

      Then("the metricPoint value should be 1")
      metricPoints.head.value shouldEqual 1


      Then("metric name should be failure-spans")
      metricPoints.head.metric shouldEqual FAILURE_METRIC_NAME


    }

    scenario("should return an empty list when error key is missing in span tags") {

      Given("a span object which missing error tag")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val process = Process.newBuilder().setServiceName(serviceName)
      val span = Span.newBuilder()
        .setDuration(duration)
        .setOperationName(operationName)
        .setProcess(process)
        .build()

      When("metricPoint is created using transformer")
      val metricPoints = mapSpan(span)

      Then("should not return metricPoints")
      metricPoints.length shouldEqual 0
    }
  }
}
