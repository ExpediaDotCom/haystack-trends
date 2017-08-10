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
package com.expedia.www.haystack.datapoints.serde

import java.util

import com.expedia.www.haystack.datapoints.entities.{DataPoint, MetricType}
import com.expedia.www.haystack.datapoints.metrics.MetricsSupport
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.json4s.jackson.Serialization.{read, write}
import org.json4s.DefaultFormats
import org.json4s.ext.EnumNameSerializer


object DataPointSerde extends Serde[DataPoint] with MetricsSupport {

  private val datapointDeserMeter = metricRegistry.meter("deseri.failure")

  implicit val formats = DefaultFormats + new EnumNameSerializer(MetricType)


  override def close(): Unit = ()

  override def deserializer(): Deserializer[DataPoint] = {
    new Deserializer[DataPoint] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def close(): Unit = ()

      /**
        * converts the json bytes into DataPoint object
        *
        * @param data serialized bytes of DataPoint
        * @return
        */
      override def deserialize(topic: String, data: Array[Byte]): DataPoint = {
        try {
          read[DataPoint](new String(data))
        } catch {
          case ex: Exception =>
            /* may be log and add metric */
            datapointDeserMeter.mark()
            null
        }
      }
    }
  }

  override def serializer(): Serializer[DataPoint] = {
    new Serializer[DataPoint] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def serialize(topic: String, dataPoint: DataPoint): Array[Byte] = {
        write(dataPoint).getBytes
      }

      override def close(): Unit = ()
    }
  }

  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()
}
