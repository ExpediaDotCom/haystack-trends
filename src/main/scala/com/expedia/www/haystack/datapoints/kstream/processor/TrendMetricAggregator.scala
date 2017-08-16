package com.expedia.www.haystack.datapoints.kstream.processor

import com.expedia.www.haystack.datapoints.aggregation.Trend
import com.expedia.www.haystack.datapoints.aggregation.metrics.{Interval, ReadOnlyMetric, TrendMetric}
import com.expedia.www.haystack.datapoints.aggregation.rules.DataPointRuleEngine
import com.expedia.www.haystack.datapoints.entities.DataPoint
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.kstream.internals._
import org.apache.kafka.streams.processor.{AbstractProcessor, Processor, ProcessorContext}
import org.apache.kafka.streams.state.KeyValueStore

class TrendMetricAggregator(computedTrendMetricsStoreName: String, trendsStoreName: String) extends KStreamAggProcessorSupplier[String, Windowed[String], DataPoint, TrendMetric] with DataPointRuleEngine {


  private var sendOldValues: Boolean = false

  def get: Processor[String, DataPoint] = {
    new TrendMetricAggregateProcessor(computedTrendMetricsStoreName, trendsStoreName)
  }

  def enableSendingOldValues() {
    sendOldValues = true
  }

  private class TrendMetricAggregateProcessor(computedTrendMetricsStoreName: String, trendsStoreName: String) extends AbstractProcessor[String, DataPoint] {
    private var computedTrendMetricsStore: KeyValueStore[String, ReadOnlyMetric] = _
    private var trendsStore: KeyValueStore[String, Trend] = _


    @SuppressWarnings(Array("unchecked"))
    override def init(context: ProcessorContext) {
      super.init(context)
      computedTrendMetricsStore = context.getStateStore(computedTrendMetricsStoreName).asInstanceOf[KeyValueStore[String, ReadOnlyMetric]]
      trendsStore = context.getStateStore(trendsStoreName).asInstanceOf[KeyValueStore[String, Trend]]
    }

    override def punctuate(timestamp: Long): Unit = {
      computedTrendMetricsStore.all().forEachRemaining((metric: KeyValue[String, ReadOnlyMetric]) => {
        metric.value.mapToDataPoints(timestamp).foreach(dataPoint => {
          context().forward(dataPoint.metric, dataPoint)
        })
        computedTrendMetricsStore.delete(metric.key)
      })
    }


    def process(key: String, value: DataPoint): Unit = {
      if (key == null) return
      // first get the matching windows

      val trend = trendsStore.get(key)
      if (trend == null) {
        val newTrend = createTrend(value)
        newTrend.compute(value)
        trendsStore.put(value.metric, newTrend)
      } else {
        trend.compute(value)
        trend.getComputedTrendMetrics.foreach(metric => {
          computedTrendMetricsStore.put(metric.getMetricKey, metric)
        })
      }
    }

    private def createTrend(value: DataPoint): Trend = {
      new Trend(findMatchingMetric(value), Interval.all, value)
    }
  }

  override def view(): KTableValueGetterSupplier[String, Trend] = new KTableValueGetterSupplier[String, Trend]() {

    override def get(): KTableValueGetter[String, Trend] = new TrendAggregateValueGetter()

    override def storeNames(): Array[String] = Array[String](trendsStoreName)

    private class TrendAggregateValueGetter extends KTableValueGetter[String, Trend] {
      private var store: KeyValueStore[String, Trend] = _

      @SuppressWarnings(Array("unchecked")) def init(context: ProcessorContext) {
        store = context.getStateStore(trendsStoreName).asInstanceOf[KeyValueStore[String, Trend]]
      }

      def get(key: String): Trend = store.get(key)
    }

  }


}





