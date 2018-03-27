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
import com.expedia.www.haystack.commons.entities.{MetricType, TagKeys}
import com.expedia.www.haystack.trends.MetricPointGenerator
import com.expedia.www.haystack.trends.exceptions.SpanValidationException
import com.expedia.www.haystack.trends.feature.FeatureSpec
import com.expedia.www.haystack.trends.transformer.{SpanDurationMetricPointTransformer, SpanStatusMetricPointTransformer}


class MetricPointGeneratorSpec extends FeatureSpec with MetricPointGenerator {


  private def getMetricPointTransformers = {
    List(SpanDurationMetricPointTransformer, SpanStatusMetricPointTransformer)
  }

  feature("The metricPoint generator must generate metricPoints given a span object") {


    scenario("any valid span object") {
      val operationName = "testSpan"
      val serviceName = "testService"
      Given("a valid span")
      val span = Span.newBuilder()
        .setDuration(System.currentTimeMillis())
        .setOperationName(operationName)
        .setServiceName(serviceName)
        .setStartTime(System.currentTimeMillis() * 1000) // in micro seconds
        .addTags(Tag.newBuilder().setKey(TagKeys.ERROR_KEY).setVBool(false))
        .build()
      When("its asked to map to metricPoints")
      val metricPoints = generateMetricPoints(blacklistedServices = List())(getMetricPointTransformers)(span, true).getOrElse(List())

      Then("the number of metricPoints returned should be equal to the number of metricPoint transformers")
      metricPoints should not be empty
      val metricPointTransformers = getMetricPointTransformers
      metricPoints.size shouldEqual metricPointTransformers.size * 2
      var metricPointIds = Set[String]()

      Then("each metricPoint should have a unique combination of keys")
      metricPoints.foreach(metricPoint => {
        metricPointIds += metricPoint.getMetricPointKey(true)
      })
      metricPointIds.size shouldEqual metricPointTransformers.size * 2

      Then("each metricPoint should have the timestamps in seconds and which should equal to the span timestamp")
      metricPoints.foreach(metricPoint => {
        metricPoint.epochTimeInSeconds shouldEqual span.getStartTime / 1000000
      })

      Then("each metricPoint should have the metric type as Metric")
      metricPoints.foreach(metricPoint => {
        metricPoint.`type` shouldEqual MetricType.Gauge
      })

    }

    scenario("an invalid span object") {
      val operationName = ""
      val serviceName = ""
      Given("an invalid span")
      val span = Span.newBuilder()
        .setDuration(System.currentTimeMillis())
        .setOperationName(operationName)
        .setServiceName(serviceName)
        .addTags(Tag.newBuilder().setKey(TagKeys.ERROR_KEY).setVBool(false))
        .build()

      When("its asked to map to metricPoints")
      val metricPoints = generateMetricPoints(blacklistedServices = List())(getMetricPointTransformers)(span, serviceOnlyFlag = true)

      Then("It should return a metricPoint validation exception")
      metricPoints.isFailure shouldBe true
      metricPoints.failed.get.isInstanceOf[SpanValidationException] shouldBe true
    }

    scenario("a span object with a valid service Name") {
      val operationName = "testSpan"
      val serviceName = "testService"

      Given("a valid span")
      val span = Span.newBuilder()
        .setDuration(System.currentTimeMillis())
        .setOperationName(operationName)
        .setServiceName(serviceName)
        .addTags(Tag.newBuilder().setKey(TagKeys.ERROR_KEY).setVBool(false))
        .build()

      When("its asked to map to metricPoints")
      val metricPoints = generateMetricPoints(blacklistedServices = List())(getMetricPointTransformers)(span, serviceOnlyFlag = false)

      Then("it should create metricPoints with service name as one its keys")
      metricPoints.isFailure shouldEqual false
      metricPoints.get.map(metricPoint => {
        metricPoint.tags.get(TagKeys.SERVICE_NAME_KEY) should not be None
        metricPoint.tags.get(TagKeys.SERVICE_NAME_KEY) shouldEqual Some(serviceName)
      })
    }

    scenario("a span object with a blacklisted service Name") {
      val operationName = "testSpan"
      val blacklistedServiceName = "testService"

      Given("a valid span with a blacklisted service name")
      val span = Span.newBuilder()
        .setDuration(System.currentTimeMillis())
        .setOperationName(operationName)
        .setServiceName(blacklistedServiceName)
        .addTags(Tag.newBuilder().setKey(TagKeys.ERROR_KEY).setVBool(false))
        .build()

      When("its asked to map to metricPoints")
      val metricPoints = generateMetricPoints(blacklistedServices = List(blacklistedServiceName))(getMetricPointTransformers)(span, serviceOnlyFlag = false)

      Then("It should return a metricPoint validation exception")
      metricPoints.isFailure shouldBe true
      metricPoints.failed.get.isInstanceOf[SpanValidationException] shouldBe true
    }
  }
}
