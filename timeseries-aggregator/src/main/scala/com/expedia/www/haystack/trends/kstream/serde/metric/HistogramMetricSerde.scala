package com.expedia.www.haystack.trends.kstream.serde.metric

import java.nio.ByteBuffer

import com.expedia.www.haystack.trends.aggregation.metrics.{HistogramMetric, Metric}
import com.expedia.www.haystack.trends.entities.Interval
import com.expedia.www.haystack.trends.entities.Interval.Interval
import org.HdrHistogram.IntHistogram
import org.msgpack.core.MessagePack
import org.msgpack.value.{Value, ValueFactory}

import scala.collection.JavaConverters._

/**
  * Serde which lets us serialize and deserilize the histogram metric, this is used when we serialize/deserialize the windowedMetric which can internally contain count or histogram metric
  * It uses messagepack to pack the object into bytes
  */
object HistogramMetricSerde extends MetricSerde {

  private val intHistogramKey = "intHistogram"
  private val intervalKey = "interval"

  override def serialize(metric: Metric): Array[Byte] = {

    val histogramMetric = metric.asInstanceOf[HistogramMetric]
    val packer = MessagePack.newDefaultBufferPacker()
    val serializedHistogram = ByteBuffer.allocate(8192)
    histogramMetric.getRunningHistogram.encodeIntoByteBuffer(serializedHistogram)
    val metricData = Map[Value, Value](
      ValueFactory.newString(intHistogramKey) -> ValueFactory.newBinary(serializedHistogram.array()),
      ValueFactory.newString(intervalKey) -> ValueFactory.newString(metric.getMetricInterval.name)
    )
    packer.packValue(ValueFactory.newMap(metricData.asJava))
    packer.toByteArray

  }

  override def deserialize(data: Array[Byte]): HistogramMetric = {
    val metric = MessagePack.newDefaultUnpacker(data).unpackValue().asMapValue().map()
    val serializedHistogram = metric.get(ValueFactory.newString(intHistogramKey)).asBinaryValue().asByteArray
    val interval: Interval = Interval.fromName(metric.get(ValueFactory.newString(intervalKey)).asStringValue().toString)
    new HistogramMetric(interval, IntHistogram.decodeFromByteBuffer(ByteBuffer.wrap(serializedHistogram), Int.MaxValue))
  }

}
