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
import com.expedia.www.haystack.datapoints.entities.{MetricType, TagKeys}
import com.expedia.www.haystack.datapoints.entities.exceptions.DataPointCreationException
import com.expedia.www.haystack.datapoints.feature.FeatureSpec
import com.expedia.www.haystack.datapoints.mapper.{DataPointGenerator, DataPointMapper}


class DataPointGeneratorSpec extends FeatureSpec with DataPointGenerator {


  private def getDataPointMappers = {
    val ref = classOf[DataPointGenerator].getInterfaces.count(interface => interface.getInterfaces.headOption.contains(classOf[DataPointMapper]))
    ref
  }

  feature("The datapoint generator must generate datapoints given a span object") {


    scenario("any valid span object") {
      val operationName = "testSpan"
      val serviceName = "testService"
      Given("a valid span")
      val span = Span.newBuilder().setDuration(System.currentTimeMillis()).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(ERROR_KEY).setVBool(false)).
        addTags(Tag.newBuilder().setKey(TagKeys.SERVICE_NAME_KEY).setVStr(serviceName)).build()
      When("its asked to map to datapoints")
      val datapoints = mapSpans(span).getOrElse(List())

      Then("the number of datapoints returned should be equal to the number of datapoint mappers")
      datapoints should not be empty
      val datapointMappers = getDataPointMappers
      datapoints.size shouldEqual datapointMappers
      var datapointIds = Set[String]()

      Then("each datapoint should have a unique combination of keys")
      datapoints.foreach(datapoint => {
        datapointIds += datapoint.getDataPointKey
      })
      datapointIds.size shouldEqual datapointMappers

      Then("each datapoint should have the timestamps which is equal to the span timestamp")
      datapoints.foreach(datapoint => {
        datapoint.timestamp shouldEqual span.getStartTime
      })

      Then("each datapoint should have the metric type as Metric")
      datapoints.foreach(datapoint => {
        datapoint.`type` shouldEqual MetricType.Metric
      })

    }

    scenario("an invalid span object") {
      val operationName = ""
      val serviceName = ""
      Given("an invalid span")
      val span = Span.newBuilder().setDuration(System.currentTimeMillis()).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(ERROR_KEY).setVBool(false)).
        addTags(Tag.newBuilder().setKey(TagKeys.SERVICE_NAME_KEY).setVStr(serviceName)).build()

      When("its asked to map to datapoints")
      val datapoints = mapSpans(span)

      Then("It should return a datapoint creation exception")
      datapoints.isFailure shouldBe true
      datapoints.failed.get.isInstanceOf[DataPointCreationException] shouldBe true
    }


    scenario("a span object with a valid operation Name") {
      val operationName = "testSpan"
      val serviceName = "testService"

      Given("a valid span")
      val span = Span.newBuilder().setDuration(System.currentTimeMillis()).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(TagKeys.SERVICE_NAME_KEY).setVStr(serviceName)).
        addTags(Tag.newBuilder().setKey(ERROR_KEY).setVBool(false)).build()

      When("its asked to map to datapoints")
      val datapoints = mapSpans(span).getOrElse(List())

      Then("it should create datapoints with operation name as one its keys")
      datapoints.map(datapoint => {
        datapoint.tags.get(TagKeys.OPERATION_NAME_KEY) should not be None
        datapoint.tags.get(TagKeys.OPERATION_NAME_KEY) shouldEqual Some(operationName)
      })
    }

    scenario("a span object with a valid service Name") {
      val operationName = "testSpan"
      val serviceName = "testService"
      Given("a valid span")
      val span = Span.newBuilder().setDuration(System.currentTimeMillis()).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(TagKeys.SERVICE_NAME_KEY).setVStr(serviceName)).
        addTags(Tag.newBuilder().setKey(ERROR_KEY).setVBool(false)).build()

      When("its asked to map to datapoints")
      val datapoints = mapSpans(span).get

      Then("it should create datapoints with service name as one its keys")
      datapoints.map(datapoint => {
        datapoint.tags.get(TagKeys.SERVICE_NAME_KEY) should not be None
        datapoint.tags.get(TagKeys.SERVICE_NAME_KEY) shouldEqual Some(serviceName)
      })
    }
  }
}
