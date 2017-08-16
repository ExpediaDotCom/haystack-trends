package com.expedia.www.haystack.datapoints.kstream

import com.expedia.www.haystack.datapoints.entities.DataPoint
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

class DataPointTimestampExtractor extends TimestampExtractor {

  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = {
    record.value().asInstanceOf[DataPoint].timestamp

  }
}
