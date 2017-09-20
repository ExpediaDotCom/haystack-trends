package com.expedia.www.haystack.trends.feature.tests.serde

import com.expedia.www.haystack.trends.aggregation.WindowedMetric
import com.expedia.www.haystack.trends.aggregation.metrics.{HistogramMetric, HistogramMetricFactory}
import com.expedia.www.haystack.trends.commons.entities.{MetricPoint, MetricType, TagKeys}
import com.expedia.www.haystack.trends.entities.Interval
import com.expedia.www.haystack.trends.entities.Interval.Interval
import com.expedia.www.haystack.trends.feature.FeatureSpec
import com.expedia.www.haystack.trends.kstream.serde.WindowedMetricSerde

class WindowedMetricSerdeSpec extends FeatureSpec {


  val DURATION_METRIC_NAME = "duration"
  val TOTAL_METRIC_NAME = "total-spans"
  val INVALID_METRIC_NAME = "invalid_metric"
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

      When("the windowed metric is serialized and then deserialized back")
      val serializedMetric = WindowedMetricSerde.serializer().serialize(TOPIC_NAME,windowedMetric)
      val deserializedMetric =  WindowedMetricSerde.deserializer().deserialize(TOPIC_NAME,serializedMetric)

      Then("Then it should deserialize the metric back in the same state")

      deserializedMetric should not be null
      windowedMetric.windowedMetricsMap.map {
        case (window, metric) =>
          deserializedMetric.windowedMetricsMap.get(window) should not be None

          val histogram = metric.asInstanceOf[HistogramMetric]
          val deserializedHistogram =  deserializedMetric.windowedMetricsMap(window).asInstanceOf[HistogramMetric]
          histogram.getMetricInterval shouldEqual deserializedHistogram.getMetricInterval
          histogram.getRunningHistogram shouldEqual deserializedHistogram.getRunningHistogram
      }



    }

    scenario("should be able to serialize and deserialize a valid windowed metric computing counts") {

      Given("some duration Metric points")

      When("get metric is constructed")

      When("MetricPoints are processed")

      Then("aggregated metric name should be the same as the MetricPoints name")

    }
  }

}
