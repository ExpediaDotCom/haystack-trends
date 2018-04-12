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

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.expedia.www.haystack.commons.health.HealthController
import com.expedia.www.haystack.commons.kstreams.serde.metricpoint.MetricTankSerde
import com.expedia.www.haystack.trends.config.ProjectConfiguration
import com.expedia.www.haystack.trends.kstream.processor.{ExternalKafkaProcessorSupplier, MetricAggProcessorSupplier}
import com.expedia.www.haystack.trends.kstream.serde.TrendMetricSerde
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.KafkaStreams.StateListener
import org.apache.kafka.streams.processor.TopologyBuilder
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters
import scala.util.Try

class StreamTopology(projectConfiguration: ProjectConfiguration) extends StateListener
  with Thread.UncaughtExceptionHandler with AutoCloseable {

  private val LOGGER = LoggerFactory.getLogger(classOf[StreamTopology])
  private val running = new AtomicBoolean(false)
  private val TOPOLOGY_SOURCE_NAME = "metricpoint-source"
  private val TOPOLOGY_EXTERNAL_SINK_NAME = "metricpoint-aggegated-sink-external"
  private val TOPOLOGY_INTERNAL_SINK_NAME = "metricpoint-aggegated-sink-internal"
  private val TOPOLOGY_AGGREGATOR_PROCESSOR_NAME = "metricpoint-aggregator-process"
  private val TOPOLOGY_AGGREGATOR_TREND_METRIC_STORE_NAME = "trend-metric-store"
  private var streams: KafkaStreams = _

  Runtime.getRuntime.addShutdownHook(new ShutdownHookThread)

  /**
    * on change event of kafka streams
    *
    * @param newState new state of kafka streams
    * @param oldState old state of kafka streams
    */
  override def onChange(newState: KafkaStreams.State, oldState: KafkaStreams.State): Unit = {
    LOGGER.info(s"State change event called with newState=$newState and oldState=$oldState")
  }

  /**
    * handle the uncaught exception by closing the current streams and rerunning it
    *
    * @param t thread which raises the exception
    * @param e throwable object
    */
  override def uncaughtException(t: Thread, e: Throwable): Unit = {
    LOGGER.error(s"uncaught exception occurred running kafka streams for thread=${t.getName}", e)
    // it may happen that uncaught exception gets called by multiple threads at the same time,
    // so we let one of them close the kafka streams and restart it
    HealthController.setUnhealthy()
  }

  /**
    * builds the topology and start kstreams
    */
  def start(): Unit = {
    if (doesConsumerTopicExist()) {
      streams = new KafkaStreams(topology(), projectConfiguration.kafkaConfig.streamsConfig)
      streams.setStateListener(this)
      streams.setUncaughtExceptionHandler(this)
      streams.cleanUp()

      // set the status healthy before starting the stream
      // Uncaught exception handler will set to unhealthy state if something goes wrong with stream
      HealthController.setHealthy()
      streams.start()
      running.set(true)
    } else {
      LOGGER.error(s"consumer topic ${projectConfiguration.kafkaConfig.consumeTopic} does not exist in kafka cluster")
      HealthController.setUnhealthy()
    }
  }

  private def topology(): TopologyBuilder = {

    val metricTankSerde = new MetricTankSerde(projectConfiguration.encoding)
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
      new MetricAggProcessorSupplier(TOPOLOGY_AGGREGATOR_TREND_METRIC_STORE_NAME, projectConfiguration.encoding),
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

  /**
    * kafka streams assume the consumer topic should be created before starting the app
    * see [http://docs.confluent.io/current/streams/developer-guide.html#user-topics]
    */
  private def doesConsumerTopicExist(): Boolean = {
    var adminClient: AdminClient = null
    try {
      val properties = new Properties()
      properties.put(
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, projectConfiguration.kafkaConfig.streamsConfig.getList(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG))
      adminClient = AdminClient.create(properties)
      // list all topics and see if it contains
      adminClient.listTopics()
        .names()
        .get()
        .contains(projectConfiguration.kafkaConfig.consumeTopic)
    } catch {
      case failure: Throwable =>
        LOGGER.error("Failed to fetch consumer topic with exception ", failure)
        false
    } finally {
      Try(adminClient.close(5, TimeUnit.SECONDS))
    }
  }


  private def closeKafkaStreams(): Boolean = {
    if (running.getAndSet(false)) {
      LOGGER.info("Closing the kafka streams.")
      streams.close(30, TimeUnit.SECONDS)
      return true
    }
    false
  }

  private class ShutdownHookThread extends Thread {
    override def run(): Unit = closeKafkaStreams()
  }


  def close(): Unit = {
    if (running.getAndSet(false)) {
      LOGGER.info("Closing the kafka streams.")
      streams.close(projectConfiguration.kafkaConfig.closeTimeoutInMs, TimeUnit.MILLISECONDS)
    }
  }

}
