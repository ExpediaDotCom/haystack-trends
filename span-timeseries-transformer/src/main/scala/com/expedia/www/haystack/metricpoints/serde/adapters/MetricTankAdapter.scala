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
package com.expedia.www.haystack.metricpoints.serde.adapters

import java.util

import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, MetricType}
import org.msgpack.MessagePack
import org.msgpack.`type`.{Value, ValueFactory}

import scala.collection.mutable

class MetricTankAdapter extends TimeseriesAdapter {

  private val msgPack = new MessagePack()

  def convertTagArrayToMap(tags: Array[String]): Map[String, String] = {
    tags.collect {
      case tag if tag.split(":").length == 2 => tag.split(":").apply(0) -> tag.split(":").apply(1)
    }.toMap
  }

  def convertTagMapToArray(tags: Map[String, String]): Array[Value] = {
    tags.map(tuple => ValueFactory.createRawValue(s"${tuple._1}:${tuple._2}")).toArray
  }

  private val idKey = "Id"
  private val orgIdKey = "OrgId"
  private val nameKey = "Name"
  private val metricKey = "Metric"
  private val valueKey = "Value"
  private val timeKey = "Time"
  private val typeKey = "Mtype"
  private val tagsKey = "Tags"
  private val intervalKey = "Interval"

  override def serializeToTimeSeriesFormat(metricPoint: MetricPoint): Array[Byte] = {
    val metricData = List[Value](
      ValueFactory.createRawValue(nameKey),
      ValueFactory.createRawValue(metricPoint.metric),
      ValueFactory.createRawValue(orgIdKey),
      ValueFactory.createIntegerValue(1),
      ValueFactory.createRawValue(intervalKey),
      ValueFactory.createIntegerValue(1),
      ValueFactory.createRawValue(metricKey),
      ValueFactory.createRawValue(metricPoint.metric),
      ValueFactory.createRawValue(valueKey),
      ValueFactory.createFloatValue(metricPoint.value),
      ValueFactory.createRawValue(timeKey),
      ValueFactory.createIntegerValue(metricPoint.timestamp),
      ValueFactory.createRawValue(typeKey),
      ValueFactory.createRawValue(metricPoint.`type`.toString),
      ValueFactory.createRawValue(tagsKey),
      ValueFactory.createArrayValue(convertTagMapToArray(metricPoint.tags))
    )


    msgPack.write(ValueFactory.createMapValue(metricData.toArray))
  }

  override def deserialize(data: Array[Byte]): MetricPoint = {
    val metricData = msgPack.read(data).asMapValue
    MetricPoint(metricData.get(ValueFactory.createRawValue(nameKey)).asRawValue().getString,
      MetricType.withName(metricData.get(ValueFactory.createRawValue(typeKey)).asRawValue().getString),
      convertTagArrayToMap(metricData.get(ValueFactory.createRawValue(tagsKey)).asArrayValue().getElementArray.map(_.asRawValue().getString)),
      metricData.get(ValueFactory.createRawValue(valueKey)).asFloatValue().getDouble.toLong,
      metricData.get(ValueFactory.createRawValue(timeKey)).asIntegerValue().getLong
    )
  }
}

