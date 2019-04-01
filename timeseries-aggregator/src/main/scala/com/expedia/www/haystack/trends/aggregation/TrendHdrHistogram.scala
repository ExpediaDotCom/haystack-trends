/*
 *
 *     Copyright 2019 Expedia, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */

package com.expedia.www.haystack.trends.aggregation

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import com.expedia.www.haystack.trends.config.entities.{HistogramMetricConfiguration, HistogramUnit}
import org.HdrHistogram.Histogram

/**
  * Wrapper over hdr Histogram. Takes care of unit mismatch of histogram and the other systems.
  *
  * @param hdrHistogram : instance of hdr Histogram
  */
class TrendHdrHistogram(hdrHistogram: Histogram, histogramUnit: HistogramUnit) {

  def this(histogramConfig: HistogramMetricConfiguration) = this(new Histogram(histogramConfig.maxValue, histogramConfig.precision), histogramConfig.unit)

  def recordValue(value: Long): Unit = {
    val metricDataValue = TrendHdrHistogram.normalizeValue(value, histogramUnit)
    hdrHistogram.recordValue(metricDataValue)
  }

  def getMinValue: Long = {
    TrendHdrHistogram.denormalizeValue(hdrHistogram.getMinValue, histogramUnit)
  }

  def getMaxValue: Long = {
    TrendHdrHistogram.denormalizeValue(hdrHistogram.getMaxValue, histogramUnit)
  }

  def getMean: Long = {
    TrendHdrHistogram.denormalizeValue(hdrHistogram.getMean.toLong, histogramUnit)
  }

  def getStdDeviation: Long = {
    TrendHdrHistogram.denormalizeValue(hdrHistogram.getStdDeviation.toLong, histogramUnit)
  }

  def getTotalCount: Long = {
    hdrHistogram.getTotalCount
  }

  def getHighestTrackableValue: Long = {
    TrendHdrHistogram.denormalizeValue(hdrHistogram.getHighestTrackableValue, histogramUnit)
  }

  def getValueAtPercentile(percentile: Double): Long = {
    TrendHdrHistogram.denormalizeValue(hdrHistogram.getValueAtPercentile(percentile), histogramUnit)
  }

  def getEstimatedFootprintInBytes: Int = {
    hdrHistogram.getEstimatedFootprintInBytes
  }

  def encodeIntoByteBuffer(buffer: ByteBuffer): Int = {
    hdrHistogram.encodeIntoByteBuffer(buffer)
  }
}

object TrendHdrHistogram {

  def normalizeValue(value: Long, histogramUnit: HistogramUnit): Long = {
    if (histogramUnit.isMillis) {
      TimeUnit.MICROSECONDS.toMillis(value)
    } else if (histogramUnit.isSeconds) {
      TimeUnit.MICROSECONDS.toSeconds(value)
    } else {
      value
    }
  }

  def denormalizeValue(value: Long, histogramUnit: HistogramUnit): Long = {
    if (histogramUnit.isMillis) {
      TimeUnit.MILLISECONDS.toMicros(value.toLong)
    } else if (histogramUnit.isSeconds) {
      TimeUnit.SECONDS.toMicros(value.toLong)
    } else {
      value
    }
  }
}
