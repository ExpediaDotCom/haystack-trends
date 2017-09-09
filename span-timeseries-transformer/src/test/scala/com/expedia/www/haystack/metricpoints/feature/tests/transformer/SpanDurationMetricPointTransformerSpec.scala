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

import com.expedia.open.tracing.{Process, Span}
import com.expedia.www.haystack.metricpoints.feature.FeatureSpec
import com.expedia.www.haystack.metricpoints.transformer.SpanDurationMetricPointTransformer

class SpanDurationMetricPointTransformerSpec extends FeatureSpec with SpanDurationMetricPointTransformer {

  feature("metricPoint transformer for creating duration metricPoint") {
    scenario("should have duration value in metricPoint for given duration in span") {

      Given("a valid span object")
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

      Then("should only have 1 metricPoint")
      metricPoints.length shouldEqual 1

      Then("same duration should be in metricPoint value")
      metricPoints.head.value shouldEqual duration


      Then("the metric name should be duration")
      metricPoints.head.metric shouldEqual DURATION_METRIC_NAME
    }
  }
}
