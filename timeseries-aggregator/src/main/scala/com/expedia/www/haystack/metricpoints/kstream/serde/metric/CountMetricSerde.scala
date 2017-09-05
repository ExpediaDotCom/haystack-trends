package com.expedia.www.haystack.metricpoints.kstream.serde.metric

import com.expedia.www.haystack.metricpoints.aggregation.metrics.Metric

object CountMetricSerde extends MetricSerde {
  override def serialize: Array[Byte] = ???

  override def deserialize(data: Array[Byte]): Metric = ???


}
