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
package com.expedia.www.haystack.metricpoints.integration.tests

import java.util.{UUID, List => JList}

import com.expedia.open.tracing.{Process, Span}
import com.expedia.www.haystack.metricpoints.StreamTopology
import com.expedia.www.haystack.metricpoints.config.entities.KafkaConfiguration
import com.expedia.www.haystack.metricpoints.entities.{MetricPoint, MetricType}
import com.expedia.www.haystack.metricpoints.integration.IntegrationTestSpec
import com.expedia.www.haystack.metricpoints.transformer.MetricPointGenerator
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.{KeyValue, StreamsConfig}

import scala.collection.JavaConverters._

class TimeSeriesTransformerTopologySpec extends IntegrationTestSpec with MetricPointGenerator {

  "TimeSeries Transformer Topology" should {

    "consume spans from input topic and transform them to metricPoints based on available transformers" in {

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

      Then("we should write transformed metricPoints to the 'output' topic")
      val metricPointKVList: JList[KeyValue[String, MetricPoint]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, 1, 15000) // get metricPoints from Kafka's output topic
      metricPointKVList.asScala.map(metricPointKV => {
        metricPointKV.value.`type` shouldEqual MetricType.Metric
      })

      Then("same metricPoints should be created as that from transformers")
      val metricPointsTransformer: List[MetricPoint] = mapSpans(span).getOrElse(List()) // directly call transformers to get metricPoints

      val metricPointSetTransformer: Set[MetricPoint] = metricPointsTransformer.toSet
      val metricPointSetKafka: Set[MetricPoint] = metricPointKVList.asScala.map(metricPointKv => metricPointKv.value).toSet

      val diffSetMetricPoint: Set[MetricPoint] = metricPointSetTransformer.diff(metricPointSetKafka)

      metricPointsTransformer.size shouldEqual (metricPointKVList.size)
      diffSetMetricPoint.isEmpty shouldEqual (true)

      Then("same keys / partition should be created as that from transformers")
      val keySetTransformer: Set[String] = metricPointsTransformer.map(metricPoint => metricPoint.getMetricPointKey).toSet
      val keySetKafka: Set[String] = metricPointKVList.asScala.map(metricPointKv => metricPointKv.key).toSet

      val diffSetKey: Set[String] = keySetTransformer.diff(keySetKafka)

      keySetTransformer.size shouldEqual (keySetKafka.size)
      diffSetKey.isEmpty shouldEqual (true)

      Then("no other intermediate partitions are created after as a result of topology")
      val adminClient: AdminClient = AdminClient.create(STREAMS_CONFIG)
      val topicNames: Iterable[String] = adminClient.listTopics.listings().get().asScala
        .map(topicListing => topicListing.name)

      topicNames.size shouldEqual (2)
      topicNames.toSet.contains(INPUT_TOPIC) shouldEqual (true)
      topicNames.toSet.contains(OUTPUT_TOPIC) shouldEqual (true)
    }
  }

  private def generateSpan(traceId: String, spanId: String, duration: Int, errorFlag: Boolean): Span = {

    val currentTime = System.currentTimeMillis()
    val process = Process.newBuilder().setServiceName("some-service")
    val span = Span.newBuilder()
      .setTraceId(traceId)
      .setParentSpanId(UUID.randomUUID().toString)
      .setSpanId(spanId)
      .setOperationName("some-op")
      .setStartTime(currentTime)
      .setDuration(duration)
      .setProcess(process)
      .addTags(com.expedia.open.tracing.Tag.newBuilder().setKey(ERROR_KEY).setVStr("some-error"))
      .build()
    span
  }
}

