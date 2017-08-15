package com.expedia.www.haystack.datapoints.kstream.serde

import java.util

import com.expedia.www.haystack.datapoints.aggregation.Trend
import com.expedia.www.haystack.datapoints.aggregation.metrics.TrendMetric
import com.expedia.www.haystack.datapoints.entities.MetricType
import com.expedia.www.haystack.datapoints.metrics.MetricsSupport
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization.{read, write}

object TrendSerde extends Serde[Trend] with MetricsSupport {

  private val datapointDeserMeter = metricRegistry.meter("deseri.failure")

  implicit val formats: Formats = DefaultFormats + new EnumNameSerializer(MetricType)


  override def close(): Unit = ()

  override def deserializer(): Deserializer[Trend] = {
    new Deserializer[Trend] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def close(): Unit = ()

      /**
        * converts the json bytes into DataPoint object
        *
        * @param data serialized bytes of DataPoint
        * @return
        */
      override def deserialize(topic: String, data: Array[Byte]): Trend = {
        try {
          read[Trend](new String(data))
        } catch {
          case ex: Exception =>
            println(ex)
            datapointDeserMeter.mark()
            null
        }
      }
    }
  }


  override def serializer(): Serializer[Trend] = {
    new Serializer[Trend] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def serialize(topic: String, trend: Trend): Array[Byte] = {
        write(trend).getBytes
      }

      override def close(): Unit = ()
    }
  }

  override def configure(map: util.Map[String, _], b: Boolean): Unit

  = ()
}