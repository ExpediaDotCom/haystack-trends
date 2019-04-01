package com.expedia.www.haystack.trends.aggregation

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import com.expedia.www.haystack.trends.config.AppConfiguration
import org.HdrHistogram.Histogram

class TrendHdrHistogram(hdrHistogram: Histogram) {

  def this() = this(new Histogram(AppConfiguration.histogramMetricConfiguration.maxValue, AppConfiguration.histogramMetricConfiguration.precision))

  def recordValue(value: Long): Unit = {
    val metricDataValue = TrendHdrHistogram.normaliseHistogramValue(value)
    hdrHistogram.recordValue(metricDataValue)
  }

  def getMinValue: Long = {
    TrendHdrHistogram.denormaliseHistogramValue(hdrHistogram.getMinValue)
  }

  def getMaxValue: Long = {
    TrendHdrHistogram.denormaliseHistogramValue(hdrHistogram.getMaxValue)
  }

  def getMean: Long = {
    TrendHdrHistogram.denormaliseHistogramValue(hdrHistogram.getMean.toLong)
  }

  def getStdDeviation: Long = {
    TrendHdrHistogram.denormaliseHistogramValue(hdrHistogram.getStdDeviation.toLong)
  }

  def getTotalCount: Long = {
    hdrHistogram.getTotalCount
  }

  def getHighestTrackableValue: Long = {
    TrendHdrHistogram.denormaliseHistogramValue(hdrHistogram.getHighestTrackableValue)
  }

  def getValueAtPercentile(percentile: Double): Long = {
    TrendHdrHistogram.denormaliseHistogramValue(hdrHistogram.getValueAtPercentile(percentile))
  }

  def getEstimatedFootprintInBytes: Int = {
    hdrHistogram.getEstimatedFootprintInBytes
  }

  def encodeIntoByteBuffer(buffer: ByteBuffer): Int = {
    hdrHistogram.encodeIntoByteBuffer(buffer)
  }
}

object TrendHdrHistogram {
  private val histogramUnit = AppConfiguration.histogramMetricConfiguration.unit
  def normaliseHistogramValue(value: Long): Long = {
    if (histogramUnit.isMillis) {
      TimeUnit.MICROSECONDS.toMillis(value)
    } else if (histogramUnit.isSeconds) {
      TimeUnit.MICROSECONDS.toSeconds(value)
    } else {
      value
    }
  }

  def denormaliseHistogramValue(value: Long): Long = {
    if (histogramUnit.isMillis) {
      TimeUnit.MILLISECONDS.toMicros(value.toLong)
    } else if (histogramUnit.isSeconds) {
      TimeUnit.SECONDS.toMicros(value.toLong)
    } else {
      value
    }
  }
}
