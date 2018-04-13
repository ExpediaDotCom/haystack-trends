package com.expedia.www.haystack.trends.feature.tests.processor

import com.expedia.www.haystack.commons.entities.Interval.Interval
import com.expedia.www.haystack.commons.entities.encoders.PeriodReplacementEncoder
import com.expedia.www.haystack.commons.entities.{Interval, MetricPoint, MetricType}
import com.expedia.www.haystack.commons.metrics.MetricsRegistries
import com.expedia.www.haystack.trends.aggregation.TrendMetric
import com.expedia.www.haystack.trends.feature.FeatureSpec
import com.expedia.www.haystack.trends.kstream.processor.MetricAggProcessorSupplier
import org.apache.kafka.streams.kstream.internals.KTableValueGetter
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.easymock.EasyMock

class MetricAggProcessorSupplierSpec extends FeatureSpec {

  feature("Metric aggregator processor supplier should return windowed metric from store") {

    val windowedMetricStoreName = "dummy-windowed-metric-store"

    scenario("should return windowed metric for a given key") {

      Given("a metric aggregator supplier and metric processor")
      val trendMetric = mock[TrendMetric]
      val metricAggProcessorSupplier = new MetricAggProcessorSupplier(windowedMetricStoreName, new PeriodReplacementEncoder)
      val keyValueStore: KeyValueStore[String, TrendMetric] = mock[KeyValueStore[String, TrendMetric]]
      val processorContext = mock[ProcessorContext]
      expecting {
        keyValueStore.get("metrics").andReturn(trendMetric)
        processorContext.getStateStore(windowedMetricStoreName).andReturn(keyValueStore)
      }
      EasyMock.replay(keyValueStore)
      EasyMock.replay(processorContext)

      When("metric processor is initialised with processor context")
      val kTableValueGetter: KTableValueGetter[String, TrendMetric] = metricAggProcessorSupplier.view().get()
      kTableValueGetter.init(processorContext)

      Then("same windowed metric should be retrieved with the given key")
      kTableValueGetter.get("metrics") shouldBe trendMetric
    }

    scenario("should not return any AggregationType for invalid MetricPoint") {

      Given("a metric aggregator supplier and an invalid metric point")
      val metricPoint = MetricPoint("invalid-metric", MetricType.Gauge, null, 80, currentTimeInSecs)
      val metricAggProcessorSupplier = new MetricAggProcessorSupplier(windowedMetricStoreName, new PeriodReplacementEncoder)

      When("find the AggregationType for the metric point")
      val aggregationType = metricAggProcessorSupplier.findMatchingMetric(metricPoint)

      Then("no AggregationType should be returned")
      aggregationType shouldEqual None
    }

    scenario("jmx metric (metricpoints.invalid) should be set for invalid MetricPoints") {
      val DURATION_METRIC_NAME = "duration"
      val validMetricPoint: MetricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, null, 10, currentTimeInSecs)
      val intervals: List[Interval] = List(Interval.ONE_MINUTE, Interval.FIFTEEN_MINUTE)
      val metricAggProcessor = new MetricAggProcessorSupplier(windowedMetricStoreName, new PeriodReplacementEncoder).get
      val metricsRegistry = MetricsRegistries.metricRegistry

      Given("metric points with invalid values")
      val negativeValueMetricPoint: MetricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, null, -1, currentTimeInSecs)
      val zeroValueMetricPoint: MetricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, null, 0, currentTimeInSecs)

      When("computing a negative value MetricPoint")
      metricAggProcessor.process(negativeValueMetricPoint.metric, negativeValueMetricPoint)

      Then("metric for invalid value should get incremented")
      metricsRegistry.getMeters.get("metricprocessor.invalid").getCount shouldEqual 1

      When("computing a zero value MetricPoint")
      metricAggProcessor.process(negativeValueMetricPoint.metric, zeroValueMetricPoint)

      Then("metric for invalid value should get incremented")
      metricsRegistry.getMeters.get("metricprocessor.invalid").getCount shouldEqual 2
    }
  }
}
