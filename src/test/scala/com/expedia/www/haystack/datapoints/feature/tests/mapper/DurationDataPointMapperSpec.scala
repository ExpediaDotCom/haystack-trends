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
import com.expedia.www.haystack.datapoints.mapper.DurationDataPointMapper

class DurationDataPointMapperSpec extends FeatureSpec with DurationDataPointMapper {

  feature("datapoint mapper for creating duration datapoint") {
    scenario("should have duration value in datapoint for given duration in span") {

      Given("a valid span object")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val span = Span.newBuilder().setDuration(duration).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(TagKeys.SERVICE_NAME_KEY).setVStr(serviceName)).build()

      When("datapoint is created using mapper")
      val dataPoints = mapSpan(span)

      Then("should only have 1 datapoint")
      dataPoints.length shouldEqual 1

      Then("same duration should be in datapoint value")
      dataPoints.head.value shouldEqual duration


      Then("the metric name should be duration")
      dataPoints.head.metric shouldEqual DURATION_METRIC_NAME
    }
  }
}
