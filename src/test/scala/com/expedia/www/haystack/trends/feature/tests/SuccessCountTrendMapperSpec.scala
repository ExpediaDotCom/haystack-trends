package com.expedia.www.haystack.trends.feature.tests

import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.trends.entities.TrendKeys
import com.expedia.www.haystack.trends.entities.TrendKeys.TrendMetric
import com.expedia.www.haystack.trends.feature.FeatureSpec
import com.expedia.www.haystack.trends.mapper.SuccessCountTrendMapper

class SuccessCountTrendMapperSpec extends FeatureSpec with SuccessCountTrendMapper {

  feature("trend mapper for creating success count trends") {

    scenario("should have success=true Trend for given span which is successful") {

      Given("a successful span object")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val span = Span.newBuilder().setDuration(duration).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(TrendKeys.SERVICE_NAME_KEY).setVStr(serviceName)).
        addTags(Tag.newBuilder().setKey(TrendKeys.ERROR_KEY).setVBool(false)).build()

      When("Trend object is created using mapper")
      val haystackTrendList = mapSpan(span)

      Then("should only have 1 trend")
      haystackTrendList.length shouldEqual 1

      Then("the count value incremented by 1")
      haystackTrendList.head.value shouldEqual 1

      Then("metricType should be COUNT")
      haystackTrendList.head.keys.get(TrendKeys.METRIC_TYPE_KEY) should not be None
      haystackTrendList.head.keys.get(TrendKeys.METRIC_TYPE_KEY) shouldEqual Some(TrendMetric.COUNT.toString)

      Then("Trend should contain error=false")
      haystackTrendList.head.keys.get(TrendKeys.ERROR_KEY) should not be None
      haystackTrendList.head.keys.get(TrendKeys.ERROR_KEY) shouldEqual Some("false")
    }

    scenario("should have success=false Trend for given span which is erroneous") {

      Given("a successful span object")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val span = Span.newBuilder().setDuration(duration).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(TrendKeys.SERVICE_NAME_KEY).setVStr(serviceName)).
        addTags(Tag.newBuilder().setKey(TrendKeys.ERROR_KEY).setVBool(true)).build()

      When("Trend object is created using mapper")
      val haystackTrendList = mapSpan(span)

      Then("should only have 1 trend")
      haystackTrendList.length shouldEqual 1

      Then("the count value incremented by 1")
      haystackTrendList.head.value shouldEqual 1

      Then("metricType should be COUNT")
      haystackTrendList.head.keys.get(TrendKeys.METRIC_TYPE_KEY) should not be None
      haystackTrendList.head.keys.get(TrendKeys.METRIC_TYPE_KEY) shouldEqual Some(TrendMetric.COUNT.toString)

      Then("Trend should contain error=true")
      haystackTrendList.head.keys.get(TrendKeys.ERROR_KEY) should not be None
      haystackTrendList.head.keys.get(TrendKeys.ERROR_KEY) shouldEqual Some("true")
    }

    scenario("should return an empty list when error key is missing in span tags") {

      Given("a span object")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val span = Span.newBuilder().setDuration(duration).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(TrendKeys.SERVICE_NAME_KEY).setVStr(serviceName)).build()

      When("Trend object is created using mapper")
      val haystackTrendList = mapSpan(span)

      Then("should not have any trends")
      haystackTrendList.length shouldEqual 0
    }
  }
}
