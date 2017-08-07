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
package com.expedia.www.haystack.datapoints.feature.tests.mapper

import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.datapoints.entities.TagKeys
import com.expedia.www.haystack.datapoints.feature.FeatureSpec
import com.expedia.www.haystack.datapoints.mapper.StatusCountDataPointMapper

class StatusCountDataPointMapperSpec extends FeatureSpec with StatusCountDataPointMapper {

  feature("datapoint mapper for creating status count datapoint") {

    scenario("should have a success-spans datapoint given span which is successful") {

      Given("a successful span object")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val span = Span.newBuilder().setDuration(duration).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(TagKeys.SERVICE_NAME_KEY).setVStr(serviceName)).
        addTags(Tag.newBuilder().setKey(ERROR_KEY).setVBool(false)).build()

      When("datapoint is created using the mapper")
      val dataPoints = mapSpan(span)

      Then("should only have 1 datapoint")
      dataPoints.length shouldEqual 1

      Then("the datapoint value should be 1")
      dataPoints.head.value shouldEqual 1

      Then("metric name should be success-spans")
      dataPoints.head.metric shouldEqual SUCCESS_METRIC_NAME
    }

    scenario("should have a failure-spans datapoint given span  which is erroneous") {

      Given("a erroneous span object")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val span = Span.newBuilder().setDuration(duration).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(TagKeys.SERVICE_NAME_KEY).setVStr(serviceName)).
        addTags(Tag.newBuilder().setKey(ERROR_KEY).setVBool(true)).build()

      When("datapoint is created using mapper")
      val dataPoints = mapSpan(span)

      Then("should only have 1 datapoint")
      dataPoints.length shouldEqual 1

      Then("the datapoint value should be 1")
      dataPoints.head.value shouldEqual 1


      Then("metric name should be failure-spans")
      dataPoints.head.metric shouldEqual FAILURE_METRIC_NAME


    }

    scenario("should return an empty list when error key is missing in span tags") {

      Given("a span object which missing error tag")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val span = Span.newBuilder().setDuration(duration).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(TagKeys.SERVICE_NAME_KEY).setVStr(serviceName)).build()

      When("datapoint is created using mapper")
      val dataPoints = mapSpan(span)

      Then("should not return datapoints")
      dataPoints.length shouldEqual 0
    }
  }
}
