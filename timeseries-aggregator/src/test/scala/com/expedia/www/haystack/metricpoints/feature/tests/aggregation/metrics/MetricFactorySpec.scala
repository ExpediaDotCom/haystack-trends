package com.expedia.www.haystack.metricpoints.feature.tests.aggregation.metrics

import com.expedia.www.haystack.metricpoints.aggregation.metrics._
import com.expedia.www.haystack.metricpoints.entities.{Interval, MetricType}
import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.feature.FeatureSpec

class MetricFactorySpec extends FeatureSpec {
  feature("Metric Factory") {

    scenario("should return a new instance of Histogram Metric") {


      Given("some an interval and gauge MetricType")
      val interval: Interval = Interval.ONE_MINUTE
      val aggregationType = AggregationType.Histogram

      When("get Metric using MetricFactory")
      val metric = MetricFactory.getMetric(aggregationType, interval)

      Then("should get Histogram MetricType")
      metric should not be None
      metric.get.getClass.getName shouldEqual classOf[HistogramMetric].getName

    }

    scenario("should return a new instance of Count Metric") {


      Given("some an interval and count MetricType")
      val interval: Interval = Interval.ONE_MINUTE
      val aggregationType = AggregationType.Count

      When("get Metric using MetricFactory")
      val metric = MetricFactory.getMetric(aggregationType, interval)

      Then("should get CountMetric")
      metric should not be None
      metric.get.getClass.getName shouldEqual classOf[CountMetric].getName


    }
  }

}
