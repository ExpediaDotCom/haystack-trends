/*
 *  Copyright 2017 Expedia, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.expedia.www.haystack.trends.transformer

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trends.commons.entities.{MetricPoint, MetricType, TagKeys}

import scala.collection.JavaConverters._


/**
  * This transformer generates a success or a failure metric y
  */
trait SpanStatusMetricPointTransformer extends MetricPointTransformer {
  private val spanFailuresMetricPoints = metricRegistry.meter("metricpoint.span.success")
  private val spanSuccessMetricPoints = metricRegistry.meter("metricpoint.span.failure")

  val SUCCESS_METRIC_NAME = "success-span"
  val FAILURE_METRIC_NAME = "failure-span"

  override def mapSpan(span: Span): List[MetricPoint] = {
    getErrorField(span) match {
      case Some(errorValue) =>

        if (errorValue) {
          spanFailuresMetricPoints.mark()
          List(MetricPoint(FAILURE_METRIC_NAME, MetricType.Gauge, createCommonMetricTags(span), 1, getDataPointTimestamp(span)))
        } else {
          spanSuccessMetricPoints.mark()
          List(MetricPoint(SUCCESS_METRIC_NAME, MetricType.Gauge, createCommonMetricTags(span), 1, getDataPointTimestamp(span)))
        }

      case None => List()
    }
  }

  protected def getErrorField(span: Span): Option[Boolean] = {
    span.getTagsList.asScala.find(tag => tag.getKey.equalsIgnoreCase(TagKeys.ERROR_KEY)).map(_.getVBool)
  }
}

object SpanStatusMetricPointTransformer extends SpanStatusMetricPointTransformer

