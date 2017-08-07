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
package com.expedia.www.haystack.datapoints.mapper

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.datapoints.entities.{DataPoint, MetricType, TagKeys}

import scala.collection.JavaConverters._

trait StatusCountDataPointMapper extends DataPointMapper {

  val SUCCESS_METRIC_NAME = "success-spans"
  val FAILURE_METRIC_NAME = "failure-spans"

  override def mapSpan(span: Span): List[DataPoint] = {
    getErrorField(span) match {
      case Some(errorValue) =>
        val tags = Map(TagKeys.OPERATION_NAME_KEY -> span.getOperationName,
          TagKeys.SERVICE_NAME_KEY -> getServiceName(span))
        if (errorValue) {
          DataPoint(FAILURE_METRIC_NAME, MetricType.Metric, tags, 1, span.getStartTime) :: super.mapSpan(span)
        } else {
          DataPoint(SUCCESS_METRIC_NAME, MetricType.Metric, tags, 1, span.getStartTime) :: super.mapSpan(span)
        }

      case None => super.mapSpan(span)
    }

  }

  protected def getErrorField(span: Span): Option[Boolean] = {
    span.getTagsList.asScala.find(tag => tag.getKey.equalsIgnoreCase(ERROR_KEY)).map(_.getVBool)
  }
}
