/*
 *
 *     Copyright 2017 Expedia, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */

package com.expedia.www.haystack.trends.commons.serde.metricpoint

import java.nio.ByteBuffer
import java.util

import com.expedia.www.haystack.trends.commons.entities.{MetricPoint, MetricType}
import com.expedia.www.haystack.trends.commons.metrics.MetricsSupport
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.msgpack.core.MessagePack.Code
import org.msgpack.core.{MessagePack, MessagePacker}
import org.msgpack.value.impl.ImmutableLongValueImpl
import org.msgpack.value.{ImmutableStringValue, Value, ValueFactory}

import scala.collection.JavaConverters._


/**
  * This class takes a metric point object and serializes it into a messagepack encoded bytestream
  * which can be directly consumed by metrictank. The serialized data is finally streamed to kafka
  */
object MetricTankSerde extends Serde[MetricPoint] with MetricsSupport {

  private val metricPointDeserMeter = metricRegistry.meter("deseri.failure")
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

  override def deserializer(): Deserializer[MetricPoint] = {
    new Deserializer[MetricPoint] {


      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      /**
        * converts the messagepack bytes into MetricPoint object
        *
        * @param data serialized bytes of MetricPoint
        * @return
        */
      override def deserialize(topic: String, data: Array[Byte]): MetricPoint = {
        try {
          val unpacker = MessagePack.newDefaultUnpacker(data)

          val metricData = unpacker.unpackValue().asMapValue().map()
          MetricPoint(
            metric = metricData.get(ValueFactory.newString(metricKey)).asStringValue().toString,
            `type` = MetricType.withName(metricData.get(ValueFactory.newString(typeKey)).asStringValue().toString),
            value = metricData.get(ValueFactory.newString(valueKey)).asFloatValue().toFloat,
            epochTimeInSeconds = metricData.get(ValueFactory.newString(timeKey)).asIntegerValue().toLong,
            tags = convertTagArrayToMap(metricData.get(ValueFactory.newString(tagsKey)).asArrayValue().iterator().asScala))
        } catch {
          case ex: Exception =>
            /* may be log and add metric */
            metricPointDeserMeter.mark()
            null
        }
      }

      override def close(): Unit = ()
    }
  }

  private def convertTagArrayToMap(tags: Iterator[Value]): Map[String, String] = {
    tags.collect {
      case tag if tag.asStringValue().toString.split(":").length == 2 => tag.asStringValue().toString.split(":").apply(0) -> tag.asStringValue().toString.split(":").apply(1)
    }.toMap
  }

  override def serializer(): Serializer[MetricPoint] = {
    new Serializer[MetricPoint] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def serialize(topic: String, metricPoint: MetricPoint): Array[Byte] = {
        val packer = MessagePack.newDefaultBufferPacker()

        val metricData = Map[Value, Value](
          ValueFactory.newString(idKey) -> ValueFactory.newString(metricPoint.getMetricPointKey),
          ValueFactory.newString(nameKey) -> ValueFactory.newString(metricPoint.getMetricPointKey),
          ValueFactory.newString(orgIdKey) -> ValueFactory.newInteger(DEFAULT_ORG_ID),
          ValueFactory.newString(intervalKey) -> ValueFactory.newInteger(DEFAULT_INTERVAL),
          ValueFactory.newString(metricKey) -> ValueFactory.newString(metricPoint.metric),
          ValueFactory.newString(valueKey) -> ValueFactory.newFloat(metricPoint.value),
          ValueFactory.newString(timeKey) -> new ImmutableSignedLongValueImpl(metricPoint.epochTimeInSeconds),
          ValueFactory.newString(typeKey) -> ValueFactory.newString(metricPoint.`type`.toString),
          ValueFactory.newString(tagsKey) -> ValueFactory.newArray(convertTagMapToArray(metricPoint.tags).asJava)
        )
        packer.packValue(ValueFactory.newMap(metricData.asJava))
        packer.toByteArray
      }

      override def close(): Unit = ()
    }
  }

  private def convertTagMapToArray(tags: Map[String, String]): List[ImmutableStringValue] = {
    tags.map {
      case (key, value) => ValueFactory.newString(s"$key:$value")
    }.toList
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()


  /**
    * This is a value extention class for signed long type. The java client for messagepack packs positive longs as unsigned
    * and there is no way to force a signed long who's numberal value is positive.
    * Metric Tank schema requres a signed long type for the timestamp key.
    * @param long
    */
  class ImmutableSignedLongValueImpl(long: Long) extends ImmutableLongValueImpl(long) {

    override def writeTo(pk: MessagePacker) {
      val buffer = ByteBuffer.allocate(java.lang.Long.BYTES + 1)
      buffer.put(Code.INT64)
      buffer.putLong(long)
      pk.addPayload(buffer.array())
    }
  }
}
