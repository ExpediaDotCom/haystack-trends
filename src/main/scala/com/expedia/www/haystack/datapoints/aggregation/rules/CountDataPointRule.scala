package com.expedia.www.haystack.datapoints.aggregation.rules

import com.expedia.www.haystack.datapoints.aggregation.metrics.{CountTrendMetric, TrendMetric}
import com.expedia.www.haystack.datapoints.entities.MetricType.MetricType
import com.expedia.www.haystack.datapoints.entities.{DataPoint, MetricType}

trait CountDataPointRule extends DataPointRule {
  override def isMatched(dataPoint: DataPoint): MetricType = {
    if (dataPoint.metric.toLowerCase.contains("count") && dataPoint.`type`.equals(MetricType.Metric)) {
      MetricType.Aggregate
    } else {
      super.isMatched(dataPoint)
    }
  }
}
