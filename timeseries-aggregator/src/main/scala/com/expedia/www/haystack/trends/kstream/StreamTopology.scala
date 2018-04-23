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

package com.expedia.www.haystack.trends.kstream

import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Supplier

import com.expedia.www.haystack.commons.kstreams.serde.metricpoint.MetricTankSerde
import com.expedia.www.haystack.trends.config.AppConfiguration
import com.expedia.www.haystack.trends.kstream.processor.{ExternalKafkaProcessorSupplier, MetricAggProcessorSupplier}
import com.expedia.www.haystack.trends.kstream.serde.TrendMetricSerde
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.processor.TopologyBuilder
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters

class StreamTopology(projectConfiguration: AppConfiguration) extends Supplier[Topology] {

  private val LOGGER = LoggerFactory.getLogger(classOf[StreamTopology])
  private val running = new AtomicBoolean(false)
  private val TOPOLOGY_SOURCE_NAME = "metricpoint-source"
  private val TOPOLOGY_EXTERNAL_SINK_NAME = "metricpoint-aggegated-sink-external"
  private val TOPOLOGY_INTERNAL_SINK_NAME = "metricpoint-aggegated-sink-internal"
  private val TOPOLOGY_AGGREGATOR_PROCESSOR_NAME = "metricpoint-aggregator-process"
  private val TOPOLOGY_AGGREGATOR_TREND_METRIC_STORE_NAME = "trend-metric-store"
  private var streams: KafkaStreams = _


  private def initialize(): TopologyBuilder = {

    val metricTankSerde = new MetricTankSerde(projectConfiguration.encoder)
    val builder = new TopologyBuilder()

    builder.addSource(
      projectConfiguration.kafkaConfig.autoOffsetReset,
      TOPOLOGY_SOURCE_NAME,
      projectConfiguration.kafkaConfig.timestampExtractor,
      new StringDeserializer,
      metricTankSerde.deserializer(),
      projectConfiguration.kafkaConfig.consumeTopic)

    val trendMetricStoreBuilder = Stores.create(TOPOLOGY_AGGREGATOR_TREND_METRIC_STORE_NAME)
      .withStringKeys
      .withValues(TrendMetricSerde)
      .inMemory()
      .maxEntries(projectConfiguration.stateStoreCacheSize)

    val trendMetricStore = {
      if (projectConfiguration.enableStateStoreLogging) {
        trendMetricStoreBuilder
          .enableLogging(JavaConverters.mapAsJavaMap(projectConfiguration.stateStoreConfig))
          .build()
      } else {
        trendMetricStoreBuilder
          .disableLogging()
          .build()
      }
    }

    builder.addProcessor(
      TOPOLOGY_AGGREGATOR_PROCESSOR_NAME,
      new MetricAggProcessorSupplier(TOPOLOGY_AGGREGATOR_TREND_METRIC_STORE_NAME, projectConfiguration.encoder),
      TOPOLOGY_SOURCE_NAME)

    builder.addStateStore(trendMetricStore, TOPOLOGY_AGGREGATOR_PROCESSOR_NAME)

    if (projectConfiguration.kafkaConfig.producerConfig.enableExternalKafka) {
      builder.addProcessor(
        TOPOLOGY_EXTERNAL_SINK_NAME,
        new ExternalKafkaProcessorSupplier(projectConfiguration.kafkaConfig.producerConfig),
        TOPOLOGY_AGGREGATOR_PROCESSOR_NAME)
    }

    builder.addSink(
      TOPOLOGY_INTERNAL_SINK_NAME,
      projectConfiguration.kafkaConfig.producerConfig.topic,
      new StringSerializer,
      metricTankSerde.serializer(),
      TOPOLOGY_AGGREGATOR_PROCESSOR_NAME)
    builder
  }

  override def get(): Topology = {
    initialize()
  }
}
