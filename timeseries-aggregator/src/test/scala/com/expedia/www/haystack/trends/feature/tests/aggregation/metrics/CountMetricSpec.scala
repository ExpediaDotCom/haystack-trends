package com.expedia.www.haystack.trends.feature.tests.aggregation.metrics

import com.expedia.www.haystack.commons.entities.Interval.Interval
import com.expedia.www.haystack.commons.entities.{Interval, MetricPoint, MetricType, TagKeys}
import com.expedia.www.haystack.trends.aggregation.metrics.{CountMetric, Metric}
import com.expedia.www.haystack.trends.aggregation.entities._
import com.expedia.www.haystack.trends.feature.FeatureSpec

class CountMetricSpec extends FeatureSpec {

  val DURATION_METRIC_NAME = "duration"
  val TOTAL_METRIC_NAME = "total-spans"
  val INVALID_METRIC_NAME = "invalid_metric"
  val SERVICE_NAME = "dummy_service"
  val OPERATION_NAME = "dummy_operation"

  val keys = Map(TagKeys.OPERATION_NAME_KEY -> OPERATION_NAME,
    TagKeys.SERVICE_NAME_KEY -> SERVICE_NAME)

  scenario("should compute the correct count for valid similar metric points") {

    Given("some 'total-spans' metric points")
    val interval: Interval = Interval.FIFTEEN_MINUTE

    val metricPoints = List(
      MetricPoint(TOTAL_METRIC_NAME, MetricType.Gauge, keys, 2, currentTimeInSecs),
      MetricPoint(TOTAL_METRIC_NAME, MetricType.Gauge, keys, 4, currentTimeInSecs),
      MetricPoint(TOTAL_METRIC_NAME, MetricType.Gauge, keys, 5, currentTimeInSecs))


    When("get metric is constructed")
    val metric: Metric = new CountMetric(interval)

    When("MetricPoints are processed")
    metricPoints.map(metricPoint => metric.compute(metricPoint))

    val countMetricPoints: List[MetricPoint] = metric.mapToMetricPoints(metricPoints.last.metric, metricPoints.last.tags, metricPoints.last.epochTimeInSeconds)


    Then("it should return a single aggregated metric point")
    countMetricPoints.size shouldBe 1
    val countMetric = countMetricPoints.head

    Then("aggregated metric name be the original metric name")
    metricPoints.foreach(metricPoint => {
      countMetric.metric shouldEqual metricPoint.metric
    })

    Then("aggregated metric should contain of original metric tags")
    metricPoints.foreach(metricPoint => {
      metricPoint.tags.foreach(tag => {
        val aggregatedMetricTag = countMetric.tags.get(tag._1)
        aggregatedMetricTag should not be None
        aggregatedMetricTag.get shouldBe tag._2
      })
    })

    Then("aggregated metric name should be the same as the metricpoint name")
    countMetricPoints
      .map(countMetricPoint =>
        countMetricPoint.metric shouldEqual countMetric.metric)

    Then("aggregated metric should count metric type in tags")
    countMetric.tags.get(TagKeys.STATS_KEY) should not be None
    countMetric.tags(TagKeys.STATS_KEY) shouldEqual StatValue.COUNT.toString

    Then("aggregated metric should contain the correct interval name in tags")
    countMetric.tags.get(TagKeys.INTERVAL_KEY) should not be None
    countMetric.tags(TagKeys.INTERVAL_KEY) shouldEqual interval.name

    Then("should return valid aggregated value for count")
    val totalSum = metricPoints.foldLeft(0f)((currentValue, point) => {
      currentValue + point.value
    })
    totalSum shouldEqual countMetric.value


  }


}
