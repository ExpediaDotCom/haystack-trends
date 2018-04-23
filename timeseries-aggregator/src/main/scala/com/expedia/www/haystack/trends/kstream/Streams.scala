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

import java.util.function.Supplier

import com.expedia.www.haystack.commons.kstreams.serde.metricpoint.MetricTankSerde
import com.expedia.www.haystack.trends.aggregation.TrendMetric
import com.expedia.www.haystack.trends.config.AppConfiguration
import com.expedia.www.haystack.trends.kstream.processor.{ExternalKafkaProcessorSupplier, MetricAggProcessorSupplier}
import com.expedia.www.haystack.trends.kstream.store.HaystackStoreBuilder
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters

class Streams(appConfiguration: AppConfiguration) extends Supplier[Topology] {

  private val LOGGER = LoggerFactory.getLogger(classOf[Streams])
  private val TOPOLOGY_SOURCE_NAME = "metricpoint-source"
  private val TOPOLOGY_EXTERNAL_SINK_NAME = "metricpoint-aggegated-sink-external"
  private val TOPOLOGY_INTERNAL_SINK_NAME = "metricpoint-aggegated-sink-internal"
  private val TOPOLOGY_AGGREGATOR_PROCESSOR_NAME = "metricpoint-aggregator-process"
  private val TOPOLOGY_AGGREGATOR_TREND_METRIC_STORE_NAME = "trend-metric-store"
  private val kafkaConfig = appConfiguration.kafkaConfig
  private val metricTankSerde = new MetricTankSerde(appConfiguration.encoder)

  private var inMemoryCache = Map[String, TrendMetric]()


  private def initialize(topology: Topology): Topology = {

    //add source - topic where the raw metricpoints are pushed by the span-timeseries-transformer
    addSource(TOPOLOGY_SOURCE_NAME, topology)


    //The processor which performs aggregations on the metrics
    topology.addProcessor(
      TOPOLOGY_AGGREGATOR_PROCESSOR_NAME,
      new MetricAggProcessorSupplier(TOPOLOGY_AGGREGATOR_TREND_METRIC_STORE_NAME, appConfiguration.encoder),
      TOPOLOGY_SOURCE_NAME)


    //key-value, state store associated with each kstreams task(partition)
    // which keeps the trend-metrics which are currently being computed in memory
    topology.addStateStore(createTrendMetricStateStore(), TOPOLOGY_AGGREGATOR_PROCESSOR_NAME)

    if (appConfiguration.kafkaConfig.producerConfig.enableExternalKafka) {
      topology.addProcessor(
        TOPOLOGY_EXTERNAL_SINK_NAME,
        new ExternalKafkaProcessorSupplier(appConfiguration.kafkaConfig.producerConfig),
        TOPOLOGY_AGGREGATOR_PROCESSOR_NAME)
    }

    topology.addSink(
      TOPOLOGY_INTERNAL_SINK_NAME,
      appConfiguration.kafkaConfig.producerConfig.topic,
      new StringSerializer,
      metricTankSerde.serializer(),
      TOPOLOGY_AGGREGATOR_PROCESSOR_NAME)
    topology
  }


  private def addSource(stepName: String, topology: Topology): Unit = {
    //add a source
    topology.addSource(
      kafkaConfig.autoOffsetReset,
      stepName,
      kafkaConfig.timestampExtractor,
      new StringDeserializer,
      metricTankSerde.deserializer(),
      kafkaConfig.consumeTopic)
  }


  private def createTrendMetricStateStore(): StoreBuilder[KeyValueStore[String, TrendMetric]] = {

    val storeBuilder = new HaystackStoreBuilder(TOPOLOGY_AGGREGATOR_TREND_METRIC_STORE_NAME, appConfiguration.stateStoreCacheSize)

    if (appConfiguration.enableStateStoreLogging) {
      storeBuilder
        .withLoggingEnabled(JavaConverters.mapAsJavaMap(appConfiguration.stateStoreConfig))

    } else {
      storeBuilder
        .withLoggingDisabled()
    }
  }


  override def get(): Topology = {
    val topology = new Topology
    initialize(topology)
  }
}
