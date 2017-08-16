package com.expedia.www.haystack.metricpoints.aggregation

import com.codahale.metrics.Meter
import com.expedia.www.haystack.metricpoints.entities.Interval.Interval
import com.expedia.www.haystack.metricpoints.aggregation.metrics.{Metric, MetricFactory}
import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, TimeWindow}
import com.expedia.www.haystack.metricpoints.entities.MetricType.MetricType
import com.expedia.www.haystack.metricpoints.metrics.MetricsSupport

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class WindowedMetric(private val metricType: MetricType, private val intervals: List[Interval], metricPoint: MetricPoint) extends MetricsSupport {


  val disorderedMetricPoints: Meter = metricRegistry.meter("disordered-metricpoints")

  //Todo: Have to add support for watermarking
  val numberOfWatermarkedWindows = 1

  val computedMetrics: ListBuffer[Metric] = mutable.ListBuffer[Metric]()

  val windowedMetricsMap: mutable.Map[TimeWindow, Metric] = createWindowedMetrics(metricPoint)
  compute(metricPoint)


  def compute(metricPoint: MetricPoint): Unit = {
    windowedMetricsMap.foreach(metricTimeWindowTuple => {
      val metricTimeWindow = metricTimeWindowTuple._1
      val metric = metricTimeWindowTuple._2

      val metricWindow = TimeWindow.apply(metricPoint.timestamp, metric.getMetricInterval)

      metricTimeWindow.compare(metricWindow) match {

        case number if number == 0 => metric.compute(metricPoint)

        case number if number < 0 =>
          windowedMetricsMap.remove(metricTimeWindow)
          windowedMetricsMap.put(metricWindow, MetricFactory.getMetric(metricType, metric.getMetricInterval).get)
          computedMetrics += metric

        case _ => disorderedMetricPoints.mark()
      }
    })
  }


  def getComputedMetricPoints: List[MetricPoint] = {
    val metricPoint = computedMetrics.toList.flatMap(metric => {
      metric.mapToMetricPoints()
    })
    computedMetrics.clear()
    metricPoint
  }


  private def createWindowedMetrics(metricPoint: MetricPoint): mutable.Map[TimeWindow, Metric] = {
    val metrics = mutable.Map[TimeWindow, Metric]()
    intervals.foreach(interval => {
      metrics.put(TimeWindow.apply(metricPoint.timestamp, interval), MetricFactory.getMetric(metricType, interval).get)
    })
    metrics
  }


}
