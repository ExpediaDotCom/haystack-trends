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

import com.expedia.www.haystack.trends.commons.entities.{Interval, MetricPoint, MetricType, TagKeys}
import com.expedia.www.haystack.trends.commons.metrics.MetricsSupport
import org.apache.commons.codec.digest.DigestUtils
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.msgpack.core.MessagePack.Code
import org.msgpack.core.{MessagePack, MessagePacker}
import org.msgpack.value.impl.ImmutableLongValueImpl
import org.msgpack.value.{Value, ValueFactory}

import scala.collection.JavaConverters._

/**
  * This class takes a metric point object and serializes it into a messagepack encoded bytestream
  * which can be directly consumed by metrictank. The serialized data is finally streamed to kafka
  */
class MetricTankSerde(enableMetricPointReplacement: Boolean) extends Serde[MetricPoint] with MetricsSupport {

  def this() = this(true)

  override def deserializer(): MetricPointDeserializer = {
    new MetricPointDeserializer(enableMetricPointReplacement)
  }

  override def serializer(): MetricPointSerializer = {
    new MetricPointSerializer(enableMetricPointReplacement)
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()
}

class MetricPointDeserializer(enableMetricPointReplacement: Boolean) extends Deserializer[MetricPoint] with MetricsSupport {

  def this() = this(true)

  private val metricPointDeserFailureMeter = metricRegistry.meter("metricpoint.deser.failure")
  private val TAG_DELIMETER = "="
  private val metricKey = "Metric"
  private val valueKey = "Value"
  private val timeKey = "Time"
  private val typeKey = "Mtype"
  private val tagsKey = "Tags"
  private val idKey = "Id"

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
        metric = createMetricNameFromMetricKey(metricData.get(ValueFactory.newString(metricKey)).asStringValue().toString),
        `type` = MetricType.withName(metricData.get(ValueFactory.newString(typeKey)).asStringValue().toString),
        value = metricData.get(ValueFactory.newString(valueKey)).asFloatValue().toFloat,
        epochTimeInSeconds = metricData.get(ValueFactory.newString(timeKey)).asIntegerValue().toLong,
        tags = createTagsFromMetricKey(metricData.get(ValueFactory.newString(metricKey)).asStringValue.toString, enableMetricPointReplacement))
    } catch {
      case ex: Exception =>
        /* may be log and add metric */
        metricPointDeserFailureMeter.mark()
        null
    }
  }

  private def createMetricNameFromMetricKey(metricKey: String): String = {
    metricKey.split("\\.").last
  }

  private def createTagsFromMetricKey(metricKey: String, enablePeriodReplacement: Boolean): Map[String, String] = {
    metricKey.split("\\.").drop(1).dropRight(1).grouped(2).map {
      if (enablePeriodReplacement) {
        tuple => tuple(0) -> tuple(1).replace("___", ".")
      } else {
        tuple => tuple(0) -> tuple(1)
      }
    }.toMap
  }

  override def close(): Unit = ()
}

class MetricPointSerializer(enableMetricPointReplacement: Boolean) extends Serializer[MetricPoint] with MetricsSupport {
  private val metricPointSerFailureMeter = metricRegistry.meter("metricpoint.ser.failure")
  private val metricPointSerSuccessMeter = metricRegistry.meter("metricpoint.ser.success")
  private val DEFAULT_ORG_ID = 1
  private[commons] val DEFAULT_INTERVAL_IN_SECS = 60
  private val idKey = "Id"
  private val orgIdKey = "OrgId"
  private val nameKey = "Name"
  private val metricKey = "Metric"
  private val valueKey = "Value"
  private val timeKey = "Time"
  private val typeKey = "Mtype"
  private val tagsKey = "Tags"
  private[commons] val intervalKey = "Interval"
  def this() = this(true)

  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

  override def serialize(topic: String, metricPoint: MetricPoint): Array[Byte] = {
    try {
      val packer = MessagePack.newDefaultBufferPacker()

      val metricData = Map[Value, Value](
        ValueFactory.newString(idKey) -> ValueFactory.newString(s"$DEFAULT_ORG_ID.${DigestUtils.md5Hex(metricPoint.getMetricPointKey(enableMetricPointReplacement).getBytes)}"),
        ValueFactory.newString(nameKey) -> ValueFactory.newString(metricPoint.getMetricPointKey(enableMetricPointReplacement)),
        ValueFactory.newString(orgIdKey) -> ValueFactory.newInteger(DEFAULT_ORG_ID),
        ValueFactory.newString(intervalKey) -> new ImmutableSignedLongValueImpl(retrieveInterval(metricPoint)),
        ValueFactory.newString(metricKey) -> ValueFactory.newString(metricPoint.getMetricPointKey(enableMetricPointReplacement)),
        ValueFactory.newString(valueKey) -> ValueFactory.newFloat(metricPoint.value),
        ValueFactory.newString(timeKey) -> new ImmutableSignedLongValueImpl(metricPoint.epochTimeInSeconds),
        ValueFactory.newString(typeKey) -> ValueFactory.newString(metricPoint.`type`.toString)
      )
      packer.packValue(ValueFactory.newMap(metricData.asJava))
      val data = packer.toByteArray
      metricPointSerSuccessMeter.mark()
      data
    } catch {
      case ex: Exception =>
        /* may be log and add metric */
        metricPointSerFailureMeter.mark()
        null
    }
  }


  //Retrieves the interval in case its present in the tags else uses the default interval
  def retrieveInterval(metricPoint: MetricPoint): Int = {
    metricPoint.tags.get(TagKeys.INTERVAL_KEY).map(stringInterval => Interval.fromName(stringInterval).timeInSeconds).getOrElse(DEFAULT_INTERVAL_IN_SECS)
  }

  override def close(): Unit = ()

  /**
    * This is a value extention class for signed long type. The java client for messagepack packs positive longs as unsigned
    * and there is no way to force a signed long who's numberal value is positive.
    * Metric Tank schema requres a signed long type for the timestamp key.
    *
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
