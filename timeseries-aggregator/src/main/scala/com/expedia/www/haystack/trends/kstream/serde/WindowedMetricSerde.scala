package com.expedia.www.haystack.trends.kstream.serde

import java.util

import com.expedia.www.haystack.trends.aggregation.WindowedMetric
import com.expedia.www.haystack.trends.aggregation.metrics.{AggregationType, CountMetricFactory, HistogramMetricFactory}
import com.expedia.www.haystack.trends.commons.metrics.MetricsSupport
import com.expedia.www.haystack.trends.entities.TimeWindow
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.msgpack.core.MessagePack
import org.msgpack.value.ValueFactory

import scala.collection.JavaConverters._
import scala.util.Try


object WindowedMetricSerde extends Serde[WindowedMetric] with MetricsSupport {

  private val windowedMetricStatsDeserMeter = metricRegistry.meter("windowedmetric.deser.failure")
  private val windowedMetricStatsSerSuccessMeter = metricRegistry.meter("windowedmetric.ser.success")
  private val windowedMetricStatsDeserSuccessMeter = metricRegistry.meter("windowedmetric.deser.success")
  private val serializedMetricKey = "serializedMetric"
  private val startTimeKey = "startTime"
  private val endTimeKey = "endTime"

  private val aggregationTypeKey = "aggregationType"
  private val metricsKey = "metrics"

  override def close(): Unit = ()

  override def deserializer(): Deserializer[WindowedMetric] = {
    new Deserializer[WindowedMetric] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def close(): Unit = ()

      /**
        * converts the messagepack encoded bytes into windowedMetric object
        *
        * @param data serialized bytes of windowedMetric
        * @return
        */
      override def deserialize(topic: String, data: Array[Byte]): WindowedMetric = {
        Try {
          val unpacker = MessagePack.newDefaultUnpacker(data)
          val serializedWindowedMetric = unpacker.unpackValue().asMapValue().map()
          val aggregationType = AggregationType.withName(serializedWindowedMetric.get(ValueFactory.newString(aggregationTypeKey)).asStringValue().toString)

          val metricFactory = aggregationType match {
            case AggregationType.Histogram => HistogramMetricFactory
            case AggregationType.Count => CountMetricFactory
          }
          val metricMap = serializedWindowedMetric.get(ValueFactory.newString(metricsKey)).asArrayValue().asScala.map(mapValue => {
            val map = mapValue.asMapValue().map()
            val startTime = map.get(ValueFactory.newString(startTimeKey)).asIntegerValue().asLong()
            val endTime = map.get(ValueFactory.newString(endTimeKey)).asIntegerValue().asLong()
            val window = TimeWindow(startTime, endTime)
            val metric = metricFactory.getMetricSerde.deserialize(map.get(ValueFactory.newString(serializedMetricKey)).asBinaryValue().asByteArray())
            window -> metric
          }).toMap

          val metric = WindowedMetric.restoreMetric(metricMap, metricFactory)
          windowedMetricStatsDeserMeter.mark()
          metric

        }.recover {
          case ex: Exception =>
            windowedMetricStatsDeserMeter.mark()
            throw ex
        }.toOption.orNull
      }
    }
  }


  override def serializer(): Serializer[WindowedMetric] = {
    new Serializer[WindowedMetric] {
      override def configure(map: util.Map[String, _], b: Boolean): Unit = ()

      override def serialize(topic: String, windowedMetric: WindowedMetric): Array[Byte] = {

        val packer = MessagePack.newDefaultBufferPacker()

        val serializedMetrics = windowedMetric.windowedMetricsMap.map {
          case (interval, metric) =>
            ValueFactory.newMap(Map(
              ValueFactory.newString(startTimeKey) -> ValueFactory.newInteger(interval.startTime),
              ValueFactory.newString(endTimeKey) -> ValueFactory.newInteger(interval.endTime),
              ValueFactory.newString(serializedMetricKey) -> ValueFactory.newBinary(windowedMetric.getMetricFactory.getMetricSerde.serialize(metric))
            ).asJava)
        }
        val windowedMetricMessagePack = Map(
          ValueFactory.newString(metricsKey) -> ValueFactory.newArray(serializedMetrics.toList.asJava),
          ValueFactory.newString(aggregationTypeKey) -> ValueFactory.newString(windowedMetric.getMetricFactory.getAggregationType.toString)
        )
        packer.packValue(ValueFactory.newMap(windowedMetricMessagePack.asJava))
        val data = packer.toByteArray
        windowedMetricStatsSerSuccessMeter.mark()
        data
      }

      override def close(): Unit = ()
    }
  }

  override def configure(map: util.Map[String, _], b: Boolean): Unit = ()
}