package com.expedia.www.haystack.trends.kstream.processor

import com.expedia.www.haystack.trends.aggregation.TrendMetric
import com.expedia.www.haystack.trends.aggregation.metrics._
import com.expedia.www.haystack.trends.aggregation.rules.MetricRuleEngine
import com.expedia.www.haystack.trends.commons.entities.{Interval, MetricPoint}
import org.apache.kafka.streams.kstream.internals._
import org.apache.kafka.streams.processor.{AbstractProcessor, Processor, ProcessorContext}
import org.apache.kafka.streams.state.KeyValueStore

class MetricAggProcessorSupplier(trendMetricStoreName: String) extends KStreamAggProcessorSupplier[String, String, MetricPoint, TrendMetric] with MetricRuleEngine {

  private var sendOldValues: Boolean = false

  def get: Processor[String, MetricPoint] = {
    new MetricAggProcessor(trendMetricStoreName)
  }

  def enableSendingOldValues() {
    sendOldValues = true
  }

  override def view(): KTableValueGetterSupplier[String, TrendMetric] = new KTableValueGetterSupplier[String, TrendMetric]() {

    override def get(): KTableValueGetter[String, TrendMetric] = new TrendMetricAggregateValueGetter()

    override def storeNames(): Array[String] = Array[String](trendMetricStoreName)

    private class TrendMetricAggregateValueGetter extends KTableValueGetter[String, TrendMetric] {

      private var store: KeyValueStore[String, TrendMetric] = _

      @SuppressWarnings(Array("unchecked")) def init(context: ProcessorContext) {
        store = context.getStateStore(trendMetricStoreName).asInstanceOf[KeyValueStore[String, TrendMetric]]
      }

      def get(key: String): TrendMetric = store.get(key)
    }

  }

  /**
    * This is the Processor which contains the map of unique trends consumed from the assigned partition and the corresponding trend metric for each trend
    * Each trend is uniquely identified by the metricPoint key - which is a combination of the name and the list of tags. Its backed by a state store which keeps this map and has the
    * ability to restore the map if/when the app restarts or when the assigned kafka partitions change
    *
    * @param trendMetricStoreName - name of the key-value state store
    */
  private class MetricAggProcessor(trendMetricStoreName: String) extends AbstractProcessor[String, MetricPoint] {
    private var trendMetricStore: KeyValueStore[String, TrendMetric] = _

    @SuppressWarnings(Array("unchecked"))
    override def init(context: ProcessorContext) {
      super.init(context)
      trendMetricStore = context.getStateStore(trendMetricStoreName).asInstanceOf[KeyValueStore[String, TrendMetric]]
    }

    /**
      * tries to fetch the trend metric based on the key, if it exists it updates the trend metric else it tries to create a new trend metric and adds it to the store      *
      *
      * @param key   - key in the kafka record - should be metricPoint.getKey
      * @param value - metricPoint
      */
    def process(key: String, value: MetricPoint): Unit = {
      if (key == null) return
      // first get the matching windows

      Option(trendMetricStore.get(key)).orElse(createTrendMetric(value)).foreach(trendMetric => {
        trendMetric.compute(value)

        /*
         we finally put the updated trend metric back to the store since we want the changelog the state store with the latest state of the trend metric, if we don't put the metric
         back and update the mutable metric, the kstreams would not capture the change and app wouldn't be able to restore to the same state when the app comes back again.
         */
        if (trendMetric.shouldLogToStateStore) {
          trendMetricStore.put(key, trendMetric)
        }

        //retrieve the computed metrics and push it to the kafka topic.
        trendMetric.getComputedMetricPoints.foreach(metricPoint => {
          context().forward(metricPoint.metric, metricPoint)
        })
      })
    }

    private def createTrendMetric(value: MetricPoint): Option[TrendMetric] = {
      findMatchingMetric(value).map {
        case AggregationType.Histogram => TrendMetric.createTrendMetric(Interval.all, value, HistogramMetricFactory)
        case AggregationType.Count => TrendMetric.createTrendMetric(Interval.all, value, CountMetricFactory)
      }
    }
  }

}





