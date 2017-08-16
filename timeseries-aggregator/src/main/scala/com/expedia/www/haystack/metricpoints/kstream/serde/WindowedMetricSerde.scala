package com.expedia.www.haystack.metricpoints.kstream.serde

import java.util

import com.expedia.www.haystack.metricpoints.aggregation.WindowedMetric
import com.expedia.www.haystack.metricpoints.entities.MetricType
import com.expedia.www.haystack.metricpoints.metrics.MetricsSupport
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization.{read, write}
import org.json4s.{DefaultFormats, Formats}

object WindowedMetricSerde extends Serde[WindowedMetric] with MetricsSupport {

  private val windowedMetricStatsDeserMeter = metricRegistry.meter("windowedmetric-deseri.failure")

  implicit val formats: Formats = DefaultFormats + new EnumNameSerializer(MetricType)


  override def close(): Unit = ()

  override def deserializer(): Deserializer[WindowedMetric] = {
    new Deserializer[WindowedMetric] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def close(): Unit = ()

      /**
        * converts the json bytes into windowedMetric object
        *
        * @param data serialized bytes of windowedMetric
        * @return
        */
      override def deserialize(topic: String, data: Array[Byte]): WindowedMetric = {
        try {
          read[WindowedMetric](new String(data))
        } catch {
          case ex: Exception =>
            println(ex)
            windowedMetricStatsDeserMeter.mark()
            null
        }
      }
    }
  }


  override def serializer(): Serializer[WindowedMetric] = {
    new Serializer[WindowedMetric] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def serialize(topic: String, windowedMetric: WindowedMetric): Array[Byte] = {
        write(windowedMetric).getBytes
      }

      override def close(): Unit = ()
    }
  }

  override def configure(map: util.Map[String, _], b: Boolean): Unit

  = ()
}