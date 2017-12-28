package com.expedia.www.haystack.trends.kstream.processor

import com.expedia.www.haystack.trends.aggregation.WindowedMetric
import com.expedia.www.haystack.trends.aggregation.metrics._
import com.expedia.www.haystack.trends.aggregation.rules.MetricRuleEngine
import com.expedia.www.haystack.trends.commons.entities.{Interval, MetricPoint}
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

  /**
    * This is the Processor which contains the map of unique trends consumed from the assigned partition and the corresponding windowed metric for each trend
    * Each trend is uniquely identified by the metricPoint key - which is a combination of the name and the list of tags. Its backed by a state store which keeps this map and has the
    * ability to restore the map if/when the app restarts or when the assigned kafka partitions change
    *
    * @param windowedMetricStoreName - name of the key-value state store
    */
  private class MetricAggProcessor(windowedMetricStoreName: String) extends AbstractProcessor[String, MetricPoint] {
    private var windowedMetricStore: KeyValueStore[String, WindowedMetric] = _


    @SuppressWarnings(Array("unchecked"))
    override def init(context: ProcessorContext) {
      super.init(context)
      windowedMetricStore = context.getStateStore(windowedMetricStoreName).asInstanceOf[KeyValueStore[String, WindowedMetric]]
    }


    /**
      * tries to fetch the windowed metric based on the key, if it exists it updates the windowed metric else it tries to create a new windowed metric and adds it to the store      *
      *
      * @param key   - key in the kafka record - should be metricPoint.getKey
      * @param value - metricPoint
      */
    def process(key: String, value: MetricPoint): Unit = {
      if (key == null) return
      // first get the matching windows

      Option(windowedMetricStore.get(key)).orElse(createWindowedMetric(value)).foreach(windowedMetric => {
        windowedMetric.compute(value)

        /*
         we finally put the updated windowed metric back to the store since we want the changelog the state store with the latest state of the windowed metric, if we don't put the metric
         back and update the mutable metric, the kstreams would not capture the change and app wouldn't be able to restore to the same state when the app comes back again.
         */
        windowedMetricStore.put(key, windowedMetric)


        //retrieve the computed metrics and push it to the kafka topic.
        windowedMetric.getComputedMetricPoints.foreach(metricPoint => {
          context().forward(metricPoint.metric, metricPoint)
        })
      })
    }

    private def createWindowedMetric(value: MetricPoint): Option[WindowedMetric] = {
      findMatchingMetric(value).map {
        case AggregationType.Histogram => WindowedMetric.createWindowedMetric(Interval.all, value, HistogramMetricFactory)
        case AggregationType.Count => WindowedMetric.createWindowedMetric(Interval.all, value, CountMetricFactory)
      }
    }
  }
}





