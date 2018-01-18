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
package com.expedia.www.haystack.trends.integration.tests

import java.util.UUID

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trends.commons.entities.{MetricPoint, MetricType, TagKeys}
import com.expedia.www.haystack.trends.config.entities.{KafkaConfiguration, TransformerConfiguration}
import com.expedia.www.haystack.trends.integration.IntegrationTestSpec
import com.expedia.www.haystack.trends.transformer.MetricPointTransformer
import com.expedia.www.haystack.trends.{MetricPointGenerator, StreamTopology}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils
import org.apache.kafka.streams.processor.TopologyBuilder.AutoOffsetReset
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.{KeyValue, StreamsConfig}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class TimeSeriesTransformerTopologySpec extends IntegrationTestSpec with MetricPointGenerator {

  "TimeSeries Transformer Topology" should {

    "consume spans from input topic and transform them to metricPoints based on available transformers" in {

      Given("a set of spans and kafka specific configurations")
      val traceId = "trace-id-dummy"
      val spanId = "span-id-dummy"
      val duration = 3
      val errorFlag = false
      val spans = generateSpans(traceId, spanId, duration, errorFlag, 10000, 8)
      val kafkaConfig = KafkaConfiguration(new StreamsConfig(STREAMS_CONFIG), OUTPUT_TOPIC, INPUT_TOPIC, AutoOffsetReset.EARLIEST, new WallclockTimestampExtractor, 30000)
      val transformerConfig = TransformerConfiguration(true, true)

      When("spans with duration and error=false are produced in 'input' topic, and kafka-streams topology is started")
      produceSpansAsync(10.millis, spans)
      new StreamTopology(kafkaConfig, transformerConfig).start()

      Then("we should write transformed metricPoints to the 'output' topic")
      val metricPoints: List[MetricPoint] = spans.flatMap(span => generateMetricPoints(MetricPointTransformer.allTransformers)(span, true).getOrElse(List())) // directly call transformers to get metricPoints
      metricPoints.size shouldBe(spans.size * MetricPointTransformer.allTransformers.size * 2) // two times because of service only metric points

      val records: List[KeyValue[String, MetricPoint]] =
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived[String, MetricPoint](RESULT_CONSUMER_CONFIG, OUTPUT_TOPIC, metricPoints.size, 15000).asScala.toList // get metricPoints from Kafka's output topic
      records.map(record => {
        record.value.`type` shouldEqual MetricType.Gauge
      })

      Then("same metricPoints should be created as that from transformers")

      val metricPointSetTransformer: Set[MetricPoint] = metricPoints.toSet
      val metricPointSetKafka: Set[MetricPoint] = records.map(metricPointKv => metricPointKv.value).toSet

      val diffSetMetricPoint: Set[MetricPoint] = metricPointSetTransformer.diff(metricPointSetKafka)

      metricPoints.size shouldEqual records.size
      diffSetMetricPoint.isEmpty shouldEqual true

      Then("same keys / partition should be created as that from transformers")
      val keySetTransformer: Set[String] = metricPoints.map(metricPoint => metricPoint.getMetricPointKey(true)).toSet
      val keySetKafka: Set[String] = records.map(metricPointKv => metricPointKv.key).toSet

      val diffSetKey: Set[String] = keySetTransformer.diff(keySetKafka)

      keySetTransformer.size shouldEqual keySetKafka.size
      diffSetKey.isEmpty shouldEqual true

      Then("no other intermediate partitions are created after as a result of topology")
      val adminClient: AdminClient = AdminClient.create(STREAMS_CONFIG)
      val topicNames: Iterable[String] = adminClient.listTopics.listings().get().asScala
        .map(topicListing => topicListing.name)

      topicNames.size shouldEqual 2
      topicNames.toSet.contains(INPUT_TOPIC) shouldEqual true
      topicNames.toSet.contains(OUTPUT_TOPIC) shouldEqual true
    }
  }

  private def generateSpans(traceId: String, spanId: String, duration: Int, errorFlag: Boolean, spanIntervalInMs: Long, spanCount: Int): List[Span] = {

    var currentTime = System.currentTimeMillis()
    for (i <- 1 to spanCount) yield {
      currentTime = currentTime + i * spanIntervalInMs

      val span = Span.newBuilder()
        .setTraceId(traceId)
        .setParentSpanId(UUID.randomUUID().toString)
        .setSpanId(spanId)
        .setOperationName("some-op")
        .setStartTime(currentTime)
        .setDuration(duration)
        .setServiceName("some-service")
        .addTags(com.expedia.open.tracing.Tag.newBuilder().setKey(TagKeys.ERROR_KEY).setVStr("some-error"))
        .build()
      span
    }
  }.toList
}

