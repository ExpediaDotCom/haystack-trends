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
import com.expedia.www.haystack.trends.commons.entities.{MetricPoint, MetricType}

/**
  * This Transformer reads a span and creates a duration metric point with the value as the
  */
trait SpanDurationMetricPointTransformer extends MetricPointTransformer {
  val DURATION_METRIC_NAME = "duration"

  override def mapSpan(span: Span): List[MetricPoint] = {
    List(MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, createCommonMetricTags(span), span.getDuration, getDataPointTimestamp(span)))
  }

}

object SpanDurationMetricPointTransformer extends SpanDurationMetricPointTransformer

