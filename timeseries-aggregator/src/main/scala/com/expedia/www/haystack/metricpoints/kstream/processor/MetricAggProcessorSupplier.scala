package com.expedia.www.haystack.metricpoints.kstream.processor

import com.expedia.www.haystack.metricpoints.aggregation.WindowedMetric
import com.expedia.www.haystack.metricpoints.aggregation.rules.MetricRuleEngine
import com.expedia.www.haystack.metricpoints.entities.{Interval, MetricPoint}
import org.apache.kafka.streams.kstream.internals._
import org.apache.kafka.streams.processor.{AbstractProcessor, Processor, ProcessorContext}
import org.apache.kafka.streams.state.KeyValueStore

class MetricAggProcessorSupplier(windowedMetricStoreName: String) extends KStreamAggProcessorSupplier[String, String, MetricPoint, WindowedMetric] with MetricRuleEngine {


  private var sendOldValues: Boolean = false

  def get: Processor[String, MetricPoint] = {
    new MetricAggProcessor(windowedMetricStoreName)
  }

  def enableSendingOldValues() {
    sendOldValues = true
  }

  private class MetricAggProcessor(windowedMetricStoreName: String) extends AbstractProcessor[String, MetricPoint] {
    private var windowedMetricStore: KeyValueStore[String, WindowedMetric] = _


    @SuppressWarnings(Array("unchecked"))
    override def init(context: ProcessorContext) {
      super.init(context)
      windowedMetricStore = context.getStateStore(windowedMetricStoreName).asInstanceOf[KeyValueStore[String, WindowedMetric]]
    }


    def process(key: String, value: MetricPoint): Unit = {
      if (key == null) return
      // first get the matching windows

      val windowedMetricStats = windowedMetricStore.get(key)
      if (windowedMetricStats == null) {
        val windowedMetric = createWindowedMetric(value)
        windowedMetric.compute(value)
        windowedMetricStore.put(value.getMetricPointKey, windowedMetric)
      } else {
        windowedMetricStats.compute(value)
        windowedMetricStats.getComputedMetricPoints.foreach(metricPoint => {
          context().forward(metricPoint.metric, metricPoint)
        })

      }
    }

    private def createWindowedMetric(value: MetricPoint): WindowedMetric = {
      new WindowedMetric(findMatchingMetric(value), Interval.all, value)
    }
  }

  override def view(): KTableValueGetterSupplier[String, WindowedMetric] = new KTableValueGetterSupplier[String, WindowedMetric]() {

    override def get(): KTableValueGetter[String, WindowedMetric] = new WindowedMetricAggregateValueGetter()

    override def storeNames(): Array[String] = Array[String](windowedMetricStoreName)

    private class WindowedMetricAggregateValueGetter extends KTableValueGetter[String, WindowedMetric] {

      private var store: KeyValueStore[String, WindowedMetric] = _

      @SuppressWarnings(Array("unchecked")) def init(context: ProcessorContext) {
        store = context.getStateStore(windowedMetricStoreName).asInstanceOf[KeyValueStore[String, WindowedMetric]]
      }

      def get(key: String): WindowedMetric = store.get(key)
    }

  }


}





