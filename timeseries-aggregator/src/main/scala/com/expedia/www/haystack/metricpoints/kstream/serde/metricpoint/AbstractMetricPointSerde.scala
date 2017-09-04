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
package com.expedia.www.haystack.metricpoints.kstream.serde.metricpoint

import java.util

import com.expedia.www.haystack.metricpoints.entities.MetricPoint
import com.expedia.www.haystack.metricpoints.metrics.MetricsSupport
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}


abstract class AbstractMetricPointSerde extends Serde[MetricPoint] with MetricsSupport {

  private val metricPointDeserMeter = metricRegistry.meter("deseri.failure")

  def deserializeMetricPoint(data:Array[Byte]):MetricPoint = ???

  def serializeMetricPoint(point:MetricPoint):Array[Byte] = ???



  override def close(): Unit = ()

  override def deserializer(): Deserializer[MetricPoint] = {
    new Deserializer[MetricPoint] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def close(): Unit = ()

      /**
        * converts the json bytes into MetricPoint object
        *
        * @param data serialized bytes of MetricPoint
        * @return
        */
      override def deserialize(topic: String, data: Array[Byte]): MetricPoint = {
        try {
          deserializeMetricPoint(data)
        } catch {
          case ex: Exception =>
            /* may be log and add metric */
            metricPointDeserMeter.mark()
            null
        }
      }
    }
  }

  override def serializer(): Serializer[MetricPoint] = {
    new Serializer[MetricPoint] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def serialize(topic: String, metricPoint: MetricPoint): Array[Byte] = {
        serializeMetricPoint(metricPoint)
      }

      override def close(): Unit = ()
    }
  }

  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()
}
