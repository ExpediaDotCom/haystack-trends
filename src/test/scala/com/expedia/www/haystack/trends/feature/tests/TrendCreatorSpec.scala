package com.expedia.www.haystack.trends.feature.tests

import com.expedia.www.haystack.trends.entities.TrendKeys
import com.expedia.www.haystack.trends.entities.exceptions.TrendCreationException
import com.expedia.www.haystack.trends.feature.FeatureSpec
import com.expedia.www.haystack.trends.mapper.{CountTrendCreator, TrendMapper}
import com.expedia.open.tracing.{Span, Tag}

import scala.util.Failure

class TrendCreatorSpec extends FeatureSpec with CountTrendCreator {

  object TestCountTrendCreator$ extends CountTrendCreator

  private def getTrendMappers() = {
    val ref = classOf[CountTrendCreator].getInterfaces.count(interface => interface.getInterfaces.headOption.contains(classOf[TrendMapper]))
    ref
  }

  feature("The Trend Creator must create trends given a Span object") {


    scenario("any valid span object") {
      val operationName = "testSpan"
      val serviceName = "testService"
      Given("a valid span")
      val span = Span.newBuilder().setDuration(System.currentTimeMillis()).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(TrendKeys.ERROR_KEY).setVBool(false)).
        addTags(Tag.newBuilder().setKey(TrendKeys.SERVICE_NAME_KEY).setVStr(serviceName)).build()
      When("its asked to map to trends")
      val trends = mapTrends(span).getOrElse(List())

      Then("it should return a size equal to the number of trend mappers")
      trends should not be empty
      val trendMappers = getTrendMappers()
      trends.size shouldEqual trendMappers
      var trendIds = Set[String]()

      Then("each trend should have a unique combination of keys")
      trends.foreach(trend => {
        trendIds += trend.getTrendKey
      })
      trendIds.size shouldEqual trendMappers

      Then("each trend should have the timestamps which is equal to the span timestamp")
      trends.foreach(trend => {
        trend.timestamp shouldEqual span.getStartTime
      })
      trendIds.size shouldEqual trendMappers

    }

    scenario("an invalid span object") {
      val operationName = ""
      val serviceName = ""
      Given("a invalid span")
      val span = Span.newBuilder().setDuration(System.currentTimeMillis()).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(TrendKeys.ERROR_KEY).setVBool(false)).
        addTags(Tag.newBuilder().setKey(TrendKeys.SERVICE_NAME_KEY).setVStr(serviceName)).build()

      When("its asked to map to trends")
      val trends = mapTrends(span)

      Then("It should return a trend creation exception")
      trends.isFailure shouldBe true
      trends match {
        case Failure(exception) => exception.isInstanceOf[TrendCreationException] shouldBe true
      }
    }


    scenario("a span object with a valid operation Name") {
      val operationName = "testSpan"
      val serviceName = "testService"

      Given("a valid span")
      val span = Span.newBuilder().setDuration(System.currentTimeMillis()).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(TrendKeys.SERVICE_NAME_KEY).setVStr(serviceName)).
        addTags(Tag.newBuilder().setKey(TrendKeys.ERROR_KEY).setVBool(false)).build()

      When("its asked to map to trends")
      val trends = mapTrends(span).getOrElse(List())

      Then("it should trends with operation name as one its keys")
      trends.map(trend => {
        trend.keys.get(TrendKeys.OPERATION_NAME_KEY) should not be None
        trend.keys.get(TrendKeys.OPERATION_NAME_KEY) shouldEqual Some(operationName)
      })
    }

    scenario("a span object with a valid service Name") {
      val operationName = "testSpan"
      val serviceName = "testService"
      Given("a valid span")
      val span = Span.newBuilder().setDuration(System.currentTimeMillis()).setOperationName(operationName).
        addTags(Tag.newBuilder().setKey(TrendKeys.SERVICE_NAME_KEY).setVStr(serviceName)).
        addTags(Tag.newBuilder().setKey(TrendKeys.ERROR_KEY).setVBool(false)).build()

      When("its asked to map to trends")
      val trends = mapTrends(span).get

      Then("it should trends with service name as one its keys")
      trends.map(trend => {
        trend.keys.get(TrendKeys.SERVICE_NAME_KEY) should not be None
        trend.keys.get(TrendKeys.SERVICE_NAME_KEY) shouldEqual Some(serviceName)
      })
    }
  }
}
