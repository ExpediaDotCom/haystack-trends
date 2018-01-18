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

package com.expedia.www.haystack.trends

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trends.commons.entities.MetricPoint
import com.expedia.www.haystack.trends.commons.health.HealthController
import com.expedia.www.haystack.trends.commons.serde.metricpoint.MetricTankSerde
import com.expedia.www.haystack.trends.config.entities.KafkaConfiguration
import com.expedia.www.haystack.trends.serde.SpanSerde
import com.expedia.www.haystack.trends.transformer.MetricPointTransformer
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.KafkaStreams.StateListener
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.processor.TopologyBuilder
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Try

class StreamTopology(kafkaConfig: KafkaConfiguration, enableMetricPointPeriodReplacement: Boolean, serviceOnlyFlag: Boolean) extends StateListener
  with Thread.UncaughtExceptionHandler with MetricPointGenerator with AutoCloseable {

  private val LOGGER = LoggerFactory.getLogger(classOf[StreamTopology])
  private val running = new AtomicBoolean(false)
  private var streams: KafkaStreams = _

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
    * kafka streams assume the consumer topic should be created before starting the app
    * see [http://docs.confluent.io/current/streams/developer-guide.html#user-topics]
    */
  private def doesConsumerTopicExist(): Boolean = {
    var adminClient: AdminClient = null
    try {
      val properties = new Properties()
      properties.put(
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.streamsConfig.getList(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG))
      adminClient = AdminClient.create(properties)
      // list all topics and see if it contains
      adminClient.listTopics()
        .names()
        .get()
        .contains(kafkaConfig.consumeTopic)
    } catch {
      case failure: Throwable =>
        LOGGER.error("Failed to fetch consumer topic with exception ", failure)
        false
    } finally {
      Try(adminClient.close(5, TimeUnit.SECONDS))
    }
  }

  /**
    * builds the topology and start kstreams
    */
  def start(): Unit = {
    LOGGER.info("Starting the kafka stream topology.")

    if (doesConsumerTopicExist()) {
      streams = new KafkaStreams(topology(), kafkaConfig.streamsConfig)
      streams.setStateListener(this)
      streams.setUncaughtExceptionHandler(this)
      streams.cleanUp()

      // set the status healthy before starting the stream
      // Uncaught exception handler will set to unhealthy state if something goes wrong with stream
      HealthController.setHealthy()
      streams.start()
      running.set(true)
    } else {
      HealthController.setUnhealthy()
    }
  }

  private def topology(): TopologyBuilder = {
    val builder = new KStreamBuilder()
    builder.stream(kafkaConfig.autoOffsetReset, kafkaConfig.timestampExtractor, new StringSerde, SpanSerde, kafkaConfig.consumeTopic)
      .flatMap[String, MetricPoint] {
      (_: String, span: Span) => mapToMetricPointKeyValue(span)
    }.to(new StringSerde, new MetricTankSerde(enableMetricPointPeriodReplacement), kafkaConfig.produceTopic)

    builder
  }

  private def mapToMetricPointKeyValue(span: Span): java.util.List[KeyValue[String, MetricPoint]] = {
    generateMetricPoints(MetricPointTransformer.allTransformers)(span, serviceOnlyFlag)
      .getOrElse(Nil)
      .map {
        metricPoint => new KeyValue(metricPoint.getMetricPointKey(enableMetricPointPeriodReplacement), metricPoint)
      }.asJava
  }

  def close(): Unit = {
    if (running.getAndSet(false)) {
      LOGGER.info("Closing the kafka streams.")
      streams.close(kafkaConfig.closeTimeoutInMs, TimeUnit.MILLISECONDS)
    }
  }
}
