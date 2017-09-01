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
package com.expedia.www.haystack.metricpoints.kstream.serde.adapters

import java.nio.ByteBuffer

import com.expedia.www.haystack.metricpoints.entities.Interval.IntervalVal
import com.expedia.www.haystack.metricpoints.entities.{Interval, MetricPoint, MetricType, TagKeys}
import org.msgpack.core.MessagePack.Code
import org.msgpack.core.{MessagePack, MessagePacker}
import org.msgpack.value.impl.ImmutableLongValueImpl
import org.msgpack.value.{ImmutableStringValue, Value, ValueFactory}

import scala.collection.JavaConverters._

class MetricTankAdapter extends TimeseriesAdapter {


  def convertTagArrayToMap(tags: Iterator[Value]): Map[String, String] = {
    tags.collect {
      case tag if tag.asStringValue().toString.split(":").length == 2 => tag.asStringValue().toString.split(":").apply(0) -> tag.asStringValue().toString.split(":").apply(1)
    }.toMap
  }

  def convertTagMapToArray(tags: Map[String, String]): List[ImmutableStringValue] = {
    tags.map(tuple => ValueFactory.newString(s"${tuple._1}:${tuple._2}")).toList
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

  private val DEFAULT_ORG_ID = 1
  private val DEFAULT_INTERVAL = 1


  def getMetricInterval(metricPoint: MetricPoint): Option[Long] = {
    metricPoint.tags.get(TagKeys.INTERVAL_KEY).map(Interval.withName(_).asInstanceOf[IntervalVal].timeInSeconds)
  }

  override def serializeToTimeSeriesFormat(metricPoint: MetricPoint): Array[Byte] = {
    val packer = MessagePack.newDefaultBufferPacker()

    val metricData = Map[Value, Value](
      ValueFactory.newString(idKey) -> ValueFactory.newString(metricPoint.getMetricPointKey),
      ValueFactory.newString(nameKey) -> ValueFactory.newString(metricPoint.getMetricPointKey),
      ValueFactory.newString(orgIdKey) -> ValueFactory.newInteger(DEFAULT_ORG_ID),
      ValueFactory.newString(intervalKey) -> new ImmutableSignedLongValueImpl(getMetricInterval(metricPoint).getOrElse(DEFAULT_INTERVAL)),
      ValueFactory.newString(metricKey) -> ValueFactory.newString(metricPoint.metric),
      ValueFactory.newString(valueKey) -> ValueFactory.newFloat(metricPoint.value),
      ValueFactory.newString(timeKey) -> new ImmutableSignedLongValueImpl(metricPoint.epochTimeInSeconds),
      ValueFactory.newString(typeKey) -> ValueFactory.newString(metricPoint.`type`.toString),
      ValueFactory.newString(tagsKey) -> ValueFactory.newArray(convertTagMapToArray(metricPoint.tags).asJava)

    )
    packer.packValue(ValueFactory.newMap(metricData.asJava))
    packer.toByteArray
  }

  override def deserialize(data: Array[Byte]): MetricPoint = {

    val unpacker = MessagePack.newDefaultUnpacker(data)

    val metricData = unpacker.unpackValue().asMapValue().map()
    MetricPoint(metric = metricData.get(ValueFactory.newString(metricKey)).asStringValue().toString, `type` = MetricType.withName(metricData.get(ValueFactory.newString(typeKey)).asStringValue().toString), tags = convertTagArrayToMap(metricData.get(ValueFactory.newString(tagsKey)).asArrayValue().iterator().asScala), value = metricData.get(ValueFactory.newString(valueKey)).asFloatValue().toFloat, epochTimeInSeconds = metricData.get(ValueFactory.newString(timeKey)).asIntegerValue().toLong)
  }


  class ImmutableSignedLongValueImpl(long: Long) extends ImmutableLongValueImpl(long) {

    override def writeTo(pk: MessagePacker) {
      val buffer = ByteBuffer.allocate(java.lang.Long.BYTES + 1)
      buffer.put(Code.INT64)
      buffer.putLong(long)
      pk.addPayload(buffer.array())
    }
  }

}




