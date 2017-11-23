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
import com.expedia.www.haystack.trends.commons.entities.{MetricPoint, TagKeys}


trait MetricPointTransformer {


  def mapSpan(span: Span): List[MetricPoint]

  protected def getDataPointTimestamp(span: Span): Long = {
    span.getStartTime / 1000000
  }

  /**
    * This function creates the common metric tags from a span object.
    * Every metric point must have the operationName and ServiceName in its tags, the individual transformer
    * can add more tags to the metricPoint.
    *
    * @param span incoming span
    * @return metric tags in the form of Map of string,string
    */
  protected def createCommonMetricTags(span: Span): Map[String, String] = {
    Map(
      TagKeys.OPERATION_NAME_KEY -> span.getOperationName,
      TagKeys.SERVICE_NAME_KEY -> span.getServiceName)
  }
}

object MetricPointTransformer {
  val allTransformers = List(SpanDurationMetricPointTransformer,SpanStatusMetricPointTransformer,SpanReceivedMetricPointTransformer)
}

