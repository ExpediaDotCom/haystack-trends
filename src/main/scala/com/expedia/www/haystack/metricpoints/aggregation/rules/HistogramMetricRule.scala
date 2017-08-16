package com.expedia.www.haystack.metricpoints.aggregation.rules

import com.expedia.www.haystack.metricpoints.entities.MetricType.MetricType
import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, MetricType}

trait HistogramMetricRule extends MetricRule {
  override def isMatched(metricPoint: MetricPoint): MetricType = {
    if (metricPoint.metric.toLowerCase.contains("duration") && metricPoint.`type`.equals(MetricType.Metric)) {
      MetricType.Histogram
    } else {
      super.isMatched(metricPoint)
    }
  }
}
