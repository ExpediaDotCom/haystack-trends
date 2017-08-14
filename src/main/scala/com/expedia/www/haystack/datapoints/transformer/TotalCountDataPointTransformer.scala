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
package com.expedia.www.haystack.datapoints.transformer

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.datapoints.entities.{DataPoint, MetricType, TagKeys}

trait TotalCountDataPointTransformer extends DataPointTransformer {
  val TOTAL_METRIC_NAME = "total-spans"

  override def mapSpan(span: Span): List[DataPoint] = {
    val keys = Map(TagKeys.OPERATION_NAME_KEY -> span.getOperationName,
      TagKeys.SERVICE_NAME_KEY -> span.getProcess.getServiceName
    )
    DataPoint(TOTAL_METRIC_NAME, MetricType.Metric, keys, 1, span.getStartTime) :: super.mapSpan(span)
  }

}
