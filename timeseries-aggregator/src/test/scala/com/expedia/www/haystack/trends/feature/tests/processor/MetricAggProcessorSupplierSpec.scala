package com.expedia.www.haystack.trends.feature.tests.processor

import com.expedia.www.haystack.trends.aggregation.TrendMetric
import com.expedia.www.haystack.trends.commons.entities.{MetricPoint, MetricType}
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
      val metricAggProcessorSupplier = new MetricAggProcessorSupplier(windowedMetricStoreName)
      val keyValueStore: KeyValueStore[String, TrendMetric] = mock[KeyValueStore[String, TrendMetric]]
      val processorContext = mock[ProcessorContext]
      expecting{
        keyValueStore.get("metrics").andReturn(trendMetric)
        processorContext.getStateStore(windowedMetricStoreName).andReturn(keyValueStore)
      }
      EasyMock.replay(keyValueStore)
      EasyMock.replay(processorContext)

      When("metric processor is initialised with processor context")
      val kTableValueGetter : KTableValueGetter[String, TrendMetric] = metricAggProcessorSupplier.view().get()
      kTableValueGetter.init(processorContext)

      Then("same windowed metric should be retrieved with the given key")
      kTableValueGetter.get("metrics") shouldBe (trendMetric)
    }

    scenario("should not return any AggregationType for invalid MetricPoint") {

      Given("a metric aggregator supplier and an invalid metric point")
      val metricPoint = MetricPoint("invalid-metric", MetricType.Gauge, null, 80, currentTimeInSecs)
      val metricAggProcessorSupplier = new MetricAggProcessorSupplier(windowedMetricStoreName)

      When("find the AggregationType for the metric point")
      val aggregationType = metricAggProcessorSupplier.findMatchingMetric(metricPoint)

      Then("no AggregationType should be returned")
      aggregationType shouldEqual None
    }
  }
}
