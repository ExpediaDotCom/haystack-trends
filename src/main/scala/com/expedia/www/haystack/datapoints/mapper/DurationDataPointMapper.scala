package com.expedia.www.haystack.datapoints.mapper

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.datapoints.entities.{DataPoint, MetricType, TagKeys}

/**
  * Created by krastogi on 04/08/17.
  */
trait DurationDataPointMapper extends DataPointMapper {
  val DURATION_METRIC_NAME = "duration"

  override def mapSpan(span: Span): List[DataPoint] = {

    val keys = Map(TagKeys.OPERATION_NAME_KEY -> span.getOperationName,
      TagKeys.SERVICE_NAME_KEY -> getServiceName(span))
    DataPoint(DURATION_METRIC_NAME,MetricType.Metric, keys, span.getDuration, span.getStartTime) :: super.mapSpan(span)
  }

}
