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
package com.expedia.www.haystack.datapoints.integration.tests

import java.util.{UUID, List => JList}

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.datapoints.StreamTopology
import com.expedia.www.haystack.datapoints.config.entities.KafkaConfiguration
import com.expedia.www.haystack.datapoints.entities.{DataPoint, MetricType, TagKeys}
import com.expedia.www.haystack.datapoints.integration.IntegrationTestSpec
import com.expedia.www.haystack.datapoints.transformer.DataPointGenerator
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.{KeyValue, StreamsConfig}

import scala.collection.JavaConverters._

class TimeSeriesTransformerTopologySpec extends IntegrationTestSpec with DataPointGenerator {

  private val MAX_DATAPOINTS = 5
  APP_ID = "haystack-topology-test"
  private val TRACE_ID = "unique-trace-id"
  private val SPAN_ID_PREFIX = "span-id"


  "TimeSeries Transformer Topology" should {

    "consume spans from input topic and transform them to datapoints based on available transformers" in {

      Given("a set of spans and kafka specific configurations")
      val traceId = "trace-id-dummy"
      val spanId = "span-id-dummy"
      val duration = 3
      val errorFlag = false
      val span = generateSpan(traceId, spanId, duration, errorFlag)
      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG), OUTPUT_TOPIC, INPUT_TOPIC, AutoOffsetReset.EARLIEST, new WallclockTimestampExtractor)

      When("spans with duration and error=false are produced in 'input' topic, and kafka-streams topology is started")
      produceSpan(span)
      new StreamTopology(kafkaConfig).start()

      Then("we should write transformed datapoints to the 'output' topic")
      val datapointKVList: JList[KeyValue[String, DataPoint]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 1, 15000) // get DataPoints from Kafka's output topic
      datapointKVList.asScala.map(datapointKV => {
        datapointKV.value.`type` shouldEqual MetricType.Metric
      })

      Then("same datapoints should be created as that from transformers")
      val dataPointsTransformer: List[DataPoint] = mapSpans(span).getOrElse(List()) // directly call transformers to get DataPoints

      val dataPointSetTransformer: Set[DataPoint] = dataPointsTransformer.toSet
      val dataPointSetKafka: Set[DataPoint] = datapointKVList.asScala.map(dataPointKv => dataPointKv.value).toSet

      val diffSetDataPoint: Set[DataPoint] = dataPointSetTransformer.diff(dataPointSetKafka)

      dataPointsTransformer.size shouldEqual (datapointKVList.size)
      diffSetDataPoint.isEmpty shouldEqual (true)

      Then("same keys / partition should be created as that from transformers")
      val keySetTransformer: Set[String] = dataPointsTransformer.map(dataPoint => dataPoint.getDataPointKey).toSet
      val keySetKafka: Set[String] = datapointKVList.asScala.map(dataPointKv => dataPointKv.key).toSet

      val diffSetKey: Set[String] = keySetTransformer.diff(keySetKafka)

      keySetTransformer.size shouldEqual (keySetKafka.size)
      diffSetKey.isEmpty shouldEqual (true)

    }
  }

  private def generateSpan(traceId: String, spanId: String, duration: Int, errorFlag: Boolean): Span = {

    val currentTime = System.currentTimeMillis()
    val span = Span.newBuilder()
      .setTraceId(traceId)
      .setParentSpanId(UUID.randomUUID().toString)
      .setSpanId(spanId)
      .setOperationName("some-op")
      .setStartTime(currentTime)
      .setDuration(duration)
      .addTags(com.expedia.open.tracing.Tag.newBuilder().setKey(TagKeys.SERVICE_NAME_KEY).setVStr("some-service"))
      .addTags(com.expedia.open.tracing.Tag.newBuilder().setKey(ERROR_KEY).setVStr("some-service"))
      .build()
    span
  }
}

