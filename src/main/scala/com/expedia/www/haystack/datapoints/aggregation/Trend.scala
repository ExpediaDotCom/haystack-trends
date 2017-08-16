package com.expedia.www.haystack.datapoints.aggregation

import com.codahale.metrics.Meter
import com.expedia.www.haystack.datapoints.aggregation.metrics.Interval.Interval
import com.expedia.www.haystack.datapoints.aggregation.metrics.{ReadOnlyMetric, TimeWindow, TrendMetric, TrendMetricFactory}
import com.expedia.www.haystack.datapoints.entities.DataPoint
import com.expedia.www.haystack.datapoints.entities.MetricType.MetricType
import com.expedia.www.haystack.datapoints.metrics.MetricsSupport

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Trend(private val metricType: MetricType, private val intervals: List[Interval], dataPoint: DataPoint) extends MetricsSupport {


  val rejectedDataPointsDueToWaterMarking: Meter = metricRegistry.meter("rejected-datapoint")

  //Todo: Have to add support for watermarking
  val numberOfWatermarkedWindows = 1

  val computedMetrics: ListBuffer[ReadOnlyMetric] = mutable.ListBuffer[ReadOnlyMetric]()

  val trendMetrics: mutable.Map[TimeWindow, TrendMetric] = createTrendMetrics(dataPoint)
  compute(dataPoint)


  def compute(dataPoint: DataPoint): Unit = {
    trendMetrics.foreach(metricTimeWindowTuple => {
      val metricTimeWindow = metricTimeWindowTuple._1
      val metric = metricTimeWindowTuple._2

      val dataPointWindow = TimeWindow.apply(dataPoint.timestamp, metric.getMetricInterval)

      metricTimeWindow.compare(dataPointWindow) match {

        case number if number == 0 => metric.compute(dataPoint)

        case number if number < 0 =>
          trendMetrics.remove(metricTimeWindow)
          trendMetrics.put(dataPointWindow, TrendMetricFactory.getTrendMetric(metricType, metric.getMetricInterval).get)
          computedMetrics += metric

        case _ => rejectedDataPointsDueToWaterMarking.mark()
      }
    })
  }


  def getComputedTrendMetrics: List[ReadOnlyMetric] = {
    val computedReadOnlyMetrics = computedMetrics.toList
    computedMetrics.clear()
    computedReadOnlyMetrics
  }


  private def createTrendMetrics(dataPoint: DataPoint): mutable.Map[TimeWindow, TrendMetric] = {
    val metrics = mutable.Map[TimeWindow, TrendMetric]()
    intervals.foreach(interval => {
      metrics.put(TimeWindow.apply(dataPoint.timestamp, interval), TrendMetricFactory.getTrendMetric(metricType, interval).get)
    })
    metrics
  }


}
