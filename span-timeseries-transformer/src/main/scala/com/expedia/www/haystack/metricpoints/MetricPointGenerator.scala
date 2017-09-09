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
package com.expedia.www.haystack.metricpoints

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.metricpoints.entities.MetricPoint
import com.expedia.www.haystack.metricpoints.entities.exceptions.SpanValidationException
import com.expedia.www.haystack.metricpoints.transformer.MetricPointTransformer

import scala.util.{Failure, Success, Try}

trait MetricPointGenerator {

  /**
    * This function is responsible for generating all the metric points which can be created given a span
    *
    * @param transformers list of transformers to be applied
    * @param span         incoming span
    * @return try of either a list of generated metric points or a validation exception
    */
  def generateMetricPoints(transformers: Seq[MetricPointTransformer])(span: Span): Try[List[MetricPoint]] = {
    validate(span).map { validatedSpan =>
      transformers.flatMap(transformer => transformer.mapSpan(validatedSpan)).toList
    }
  }

  /**
    * This function validates a span and makes sure that the span has the necessary data to generate meaningful metrics
    * This layer is supposed to do generic validations which would impact all the transformers.
    * Validation specific to the transformer can be done in the transformer itself
    *
    * @param span incoming span
    * @return Try object which should return either the span as is or a validation exception
    */
  private def validate(span: Span): Try[Span] = {
    if (span.getProcess.getServiceName.isEmpty || span.getOperationName.isEmpty) {
      Failure(new SpanValidationException)
    } else {
      Success(span)
    }
  }


}





