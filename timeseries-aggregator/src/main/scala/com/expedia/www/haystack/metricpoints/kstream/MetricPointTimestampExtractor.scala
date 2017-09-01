package com.expedia.www.haystack.metricpoints.kstream

import com.expedia.www.haystack.metricpoints.entities.MetricPoint
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

class MetricPointTimestampExtractor extends TimestampExtractor {

  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = {
    record.value().asInstanceOf[MetricPoint].epochTimeInSeconds

  }
}
