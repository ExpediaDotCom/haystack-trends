package com.expedia.www.haystack.trends.feature.tests.serde

import com.expedia.www.haystack.trends.aggregation.WindowedMetric
import com.expedia.www.haystack.trends.aggregation.metrics.{CountMetric, CountMetricFactory, HistogramMetric, HistogramMetricFactory}
import com.expedia.www.haystack.trends.commons.entities.Interval.Interval
import com.expedia.www.haystack.trends.commons.entities.{Interval, MetricPoint, MetricType, TagKeys}
import com.expedia.www.haystack.trends.feature.FeatureSpec
import com.expedia.www.haystack.trends.kstream.serde.WindowedMetricSerde

class WindowedMetricSerdeSpec extends FeatureSpec {


  val DURATION_METRIC_NAME = "duration"
  val TOTAL_METRIC_NAME = "total-spans"
  val SERVICE_NAME = "dummy_service"
  val TOPIC_NAME = "dummy"
  val OPERATION_NAME = "dummy_operation"
  val keys = Map(TagKeys.OPERATION_NAME_KEY -> OPERATION_NAME,
    TagKeys.SERVICE_NAME_KEY -> SERVICE_NAME)

  feature("Serializing/Deserializing Windowed Metric") {

    scenario("should be able to serialize and deserialize a valid windowed metric computing histograms") {
      val durations: List[Long] = List(10, 140)
      val intervals: List[Interval] = List(Interval.ONE_MINUTE, Interval.FIFTEEN_MINUTE)

      val metricPoints: List[MetricPoint] = durations.map(duration => MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, keys, duration, currentTimeInSecs))

      When("creating a WindowedMetric and passing some MetricPoints and aggregation type as Histogram")
      val windowedMetric: WindowedMetric = WindowedMetric.createWindowedMetric(intervals, metricPoints.head, HistogramMetricFactory)
      metricPoints.indices.foreach(i => if (i > 0) {
        windowedMetric.compute(metricPoints(i))
      })
      val expectedMetricsMap = windowedMetric.windowedMetrics.flatMap(tuple => List(tuple._2)).flatten.map {
        case (timeWindow, metric) =>
          timeWindow -> metric
      }.toMap

      When("the windowed metric is serialized and then deserialized back")
      val serializedMetric = WindowedMetricSerde.serializer().serialize(TOPIC_NAME, windowedMetric)
      val deserializedMetric = WindowedMetricSerde.deserializer().deserialize(TOPIC_NAME, serializedMetric)

      Then("Then it should deserialize the metric back in the same state")
      deserializedMetric should not be null
      deserializedMetric.windowedMetrics.flatMap(tuple => List(tuple._2)).flatten.map {
        case (timeWindow, deserializedMetric) =>
          deserializedMetric.getMetricInterval shouldEqual expectedMetricsMap(timeWindow).getMetricInterval
          val deserializedHistMetric = deserializedMetric.asInstanceOf[HistogramMetric]
          val expectedHistMetric = expectedMetricsMap(timeWindow).asInstanceOf[HistogramMetric]
          deserializedHistMetric.getRunningHistogram shouldEqual expectedHistMetric.getRunningHistogram
      }
    }

    scenario("should be able to serialize and deserialize a valid windowed metric computing counts") {

      Given("some count Metric points")
      val counts: List[Long] = List(10, 140)
      val intervals: List[Interval] = List(Interval.ONE_MINUTE, Interval.FIFTEEN_MINUTE)
      val metricPoints: List[MetricPoint] = counts.map(count => MetricPoint(TOTAL_METRIC_NAME, MetricType.Gauge, keys, count, currentTimeInSecs))


      When("creating a WindowedMetric and passing some MetricPoints and aggregation type as Count")
      val windowedMetric: WindowedMetric = WindowedMetric.createWindowedMetric(intervals, metricPoints.head, CountMetricFactory)
      metricPoints.indices.foreach(i => if (i > 0) {
        windowedMetric.compute(metricPoints(i))
      })
      val expectedMetricsMap = windowedMetric.windowedMetrics.flatMap(tuple => List(tuple._2)).flatten.map {
        case (timeWindow, metric) =>
          timeWindow -> metric
      }.toMap

      When("the windowed metric is serialized and then deserialized back")
      val serializer = WindowedMetricSerde.serializer()
      val deserializer = WindowedMetricSerde.deserializer()
      val serializedMetric = serializer.serialize(TOPIC_NAME, windowedMetric)
      val deserializedMetric = deserializer.deserialize(TOPIC_NAME, serializedMetric)

      Then("Then it should deserialize the metric back in the same state")
      deserializedMetric should not be null
      deserializedMetric.windowedMetrics.flatMap(tuple => List(tuple._2)).flatten.map {
        case (timeWindow, deserializedMetric) =>
          deserializedMetric.getMetricInterval shouldEqual expectedMetricsMap(timeWindow).getMetricInterval
          val deserializedCountMetric = deserializedMetric.asInstanceOf[CountMetric]
          val expectedCountMetric = expectedMetricsMap(timeWindow).asInstanceOf[CountMetric]
          deserializedCountMetric.getCurrentCount shouldEqual expectedCountMetric.getCurrentCount
      }
      serializer.close()
      deserializer.close()
      WindowedMetricSerde.close()
    }
  }

}
