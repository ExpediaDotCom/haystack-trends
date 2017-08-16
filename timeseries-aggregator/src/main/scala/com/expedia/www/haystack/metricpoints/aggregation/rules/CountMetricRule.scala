package com.expedia.www.haystack.metricpoints.aggregation.rules

import com.expedia.www.haystack.metricpoints.entities.MetricType.MetricType
import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, MetricType}

trait CountMetricRule extends MetricRule {
  override def isMatched(metricPoint: MetricPoint): MetricType = {
    if (metricPoint.metric.toLowerCase.contains("count") && metricPoint.`type`.equals(MetricType.Metric)) {
      MetricType.Aggregate
    } else {
      super.isMatched(metricPoint)
    }
  }
}
