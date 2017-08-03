package com.expedia.www.haystack.trends.feature.tests

import com.expedia.open.tracing.{Span, Tag}
import com.expedia.www.haystack.trends.entities.TrendKeys
import com.expedia.www.haystack.trends.entities.TrendKeys.TrendMetric
import com.expedia.www.haystack.trends.feature.FeatureSpec
import com.expedia.www.haystack.trends.mapper.DurationTrendMapper

class DurationTrendMapperSpec  extends FeatureSpec with DurationTrendMapper {

  feature("trend mapper for creating duration trends") {
    scenario("should have duration value in Trend for given duration in span") {

      Given("a valid span object")
      val operationName = "testSpan"
      val serviceName = "testService"
      val duration = System.currentTimeMillis
      val span = Span.newBuilder().setDuration(duration).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(TrendKeys.SERVICE_NAME_KEY).setVStr(serviceName)).build()

      When("Trend object is created using mapper")
      val haystackTrendList = mapSpan(span)

      Then ("should only have 1 trend")
      haystackTrendList.length shouldEqual 1

      Then ("same duration should be in trend value")
      haystackTrendList.head.value shouldEqual duration

      Then ("metricType should be HISTOGRAM")
      haystackTrendList.head.keys.get(TrendKeys.METRIC_TYPE_KEY) should not be None
      haystackTrendList.head.keys.get(TrendKeys.METRIC_TYPE_KEY) shouldEqual Some(TrendMetric.HISTOGRAM.toString)

      Then ("metric field should be duration")
      haystackTrendList.head.keys.get(TrendKeys.METRIC_FIELD_KEY) shouldEqual Some("duration")
    }
  }
}
