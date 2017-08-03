package com.expedia.www.haystack.trends.mapper

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trends.entities.TrendKeys.TrendMetric
import com.expedia.www.haystack.trends.entities.{HaystackTrend, TrendKeys}

import scala.collection.JavaConverters._


trait TrendMapper {

  def mapSpan(span: Span): List[HaystackTrend] = List()


  protected def getServiceName(span: Span): String = {
    span.getTagsList.asScala.find(tag => tag.getKey.equalsIgnoreCase(TrendKeys.SERVICE_NAME_KEY)).map(_.getVStr).getOrElse("")
  }

  protected def getErrorField(span: Span): Option[Boolean] = {
    span.getTagsList.asScala.find(tag => tag.getKey.equalsIgnoreCase(TrendKeys.ERROR_KEY)).map(_.getVBool)
  }
}

trait DurationTrendMapper extends TrendMapper {

  override def mapSpan(span: Span): List[HaystackTrend] = {

    val keys = Map(TrendKeys.OPERATION_NAME_KEY -> span.getOperationName,
      TrendKeys.SERVICE_NAME_KEY -> getServiceName(span),
      TrendKeys.METRIC_TYPE_KEY -> TrendMetric.HISTOGRAM.toString,
      TrendKeys.METRIC_FIELD_KEY -> "duration")
    HaystackTrend(keys, span.getDuration, span.getStartTime) :: super.mapSpan(span)
  }
}


trait TotalCountTrendMapper extends TrendMapper {

  override def mapSpan(span: Span): List[HaystackTrend] = {
    val keys = Map(TrendKeys.OPERATION_NAME_KEY -> span.getOperationName,
      TrendKeys.SERVICE_NAME_KEY -> getServiceName(span),
      TrendKeys.METRIC_TYPE_KEY -> TrendMetric.COUNT.toString
    )
    HaystackTrend(keys, 1, span.getStartTime) :: super.mapSpan(span)
  }
}


trait SuccessCountTrendMapper extends TrendMapper {
  override def mapSpan(span: Span): List[HaystackTrend] = {
    getErrorField(span) match {
      case Some(errorValue) =>
        val keys = Map(TrendKeys.OPERATION_NAME_KEY -> span.getOperationName,
          TrendKeys.SERVICE_NAME_KEY -> getServiceName(span),
          TrendKeys.METRIC_TYPE_KEY -> TrendMetric.COUNT.toString,
          TrendKeys.ERROR_KEY -> errorValue.toString)
        HaystackTrend(keys, 1, span.getStartTime) :: super.mapSpan(span)
      case None => super.mapSpan(span)

    }
  }
}