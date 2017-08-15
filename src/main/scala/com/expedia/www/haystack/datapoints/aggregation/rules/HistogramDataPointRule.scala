package com.expedia.www.haystack.datapoints.aggregation.rules

import com.expedia.www.haystack.datapoints.entities.MetricType.MetricType
import com.expedia.www.haystack.datapoints.entities.{DataPoint, MetricType}

trait HistogramDataPointRule extends DataPointRule {
  override def isMatched(dataPoint: DataPoint): MetricType = {
    if (dataPoint.metric.toLowerCase.contains("duration") && dataPoint.`type`.equals(MetricType.Metric)) {
      MetricType.Histogram
    } else {
      super.isMatched(dataPoint)
    }
  }
}
