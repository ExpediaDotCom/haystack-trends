package com.expedia.www.haystack.metricpoints.kstream.serde.metric

import com.expedia.www.haystack.metricpoints.aggregation.metrics.Metric

trait MetricSerde {
  def serialize: Array[Byte] = ???

  def deserialize(data: Array[Byte]): Metric = ???


}
