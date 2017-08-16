package com.expedia.www.haystack.datapoints.kstream.serde

import java.util

import com.expedia.www.haystack.datapoints.aggregation.metrics.{CountTrendMetric, TrendMetric}
import com.expedia.www.haystack.datapoints.entities.MetricType
import com.expedia.www.haystack.datapoints.metrics.MetricsSupport
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization.{read, write}

object TrendMetricSerde extends Serde[TrendMetric] with MetricsSupport {

  private val datapointDeserMeter = metricRegistry.meter("deseri.failure")

  implicit val formats: Formats = DefaultFormats + new EnumNameSerializer(MetricType)


  override def close(): Unit = ()

  override def deserializer(): Deserializer[TrendMetric] = {
    new Deserializer[TrendMetric] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def close(): Unit = ()

      /**
        * converts the json bytes into DataPoint object
        *
        * @param data serialized bytes of DataPoint
        * @return
        */
      override def deserialize(topic: String, data: Array[Byte]): TrendMetric = {
        try {
          read[CountTrendMetric](new String(data))
        } catch {
          case ex: Exception =>
            println(ex)
            datapointDeserMeter.mark()
            null
        }
      }
    }
  }


  override def serializer(): Serializer[TrendMetric] = {
    new Serializer[TrendMetric] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def serialize(topic: String, trendMetric: TrendMetric): Array[Byte] = {
        write(trendMetric).getBytes
      }

      override def close(): Unit = ()
    }
  }

  override def configure(map: util.Map[String, _], b: Boolean): Unit

  = ()
}