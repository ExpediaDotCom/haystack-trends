package com.expedia.www.haystack.trends.mapper

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trends.entities.HaystackTrend
import com.expedia.www.haystack.trends.entities.exceptions.TrendCreationException

import scala.util.{Failure, Success, Try}

trait CountTrendCreator extends DurationTrendMapper with TotalCountTrendMapper with SuccessCountTrendMapper {

  def mapTrends(span: Span): Try[List[HaystackTrend]] = {

    validate(span).map(span => mapSpan(span))
  }

  private def validate(span: Span): Try[Span] = {
    if (getServiceName(span).isEmpty || span.getOperationName.isEmpty) {
      Failure(new TrendCreationException)
    } else {
      Success(span)
    }
  }


}





