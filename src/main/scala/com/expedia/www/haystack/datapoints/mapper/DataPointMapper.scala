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
import com.expedia.www.haystack.datapoints.entities.{DataPoint, TagKeys}

import scala.collection.JavaConverters._

trait DataPointMapper {
  val ERROR_KEY = "error"

  def mapSpan(span: Span): List[DataPoint] = List()

  protected def getServiceName(span: Span): String = {
    span.getTagsList.asScala.find(tag => tag.getKey.equalsIgnoreCase(TagKeys.SERVICE_NAME_KEY)).map(_.getVStr).getOrElse("")
  }

}
