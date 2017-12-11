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
package com.expedia.www.haystack.trends.integration

import java.util.Properties
import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import com.expedia.www.haystack.trends.commons.entities.{MetricPoint, MetricType}
import com.expedia.www.haystack.trends.commons.serde.metricpoint.MetricTankSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.integration.utils.{EmbeddedKafkaCluster, IntegrationTestUtils}
import org.apache.kafka.streams.{KeyValue, StreamsConfig}
import org.scalatest._

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

object EmbeddedKafka {
  val CLUSTER = new EmbeddedKafkaCluster(1)
  CLUSTER.start()
}

class IntegrationTestSpec extends WordSpec with GivenWhenThen with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {


  protected val PUNCTUATE_INTERVAL_SEC = 2000
  protected val PRODUCER_CONFIG = new Properties()
  protected val RESULT_CONSUMER_CONFIG = new Properties()
  protected val STREAMS_CONFIG = new Properties()
  protected val scheduledJobFuture: ScheduledFuture[_] = null
  protected val INPUT_TOPIC = "metricpoints"
  protected val OUTPUT_TOPIC = "aggregatedmetricpoints"
  protected var scheduler: ScheduledExecutorService = _
  protected var APP_ID = "haystack-trends"
  protected var CHANGELOG_TOPIC = ""

  override def beforeAll() {
    scheduler = Executors.newScheduledThreadPool(2)
  }

  override def afterAll(): Unit = {
    scheduler.shutdownNow()
  }

  override def beforeEach() {
    EmbeddedKafka.CLUSTER.createTopic(INPUT_TOPIC)
    EmbeddedKafka.CLUSTER.createTopic(OUTPUT_TOPIC)

    PRODUCER_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, EmbeddedKafka.CLUSTER.bootstrapServers)
    PRODUCER_CONFIG.put(ProducerConfig.ACKS_CONFIG, "all")
    PRODUCER_CONFIG.put(ProducerConfig.RETRIES_CONFIG, "0")
    PRODUCER_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MetricTankSerde.serializer().getClass)

    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, EmbeddedKafka.CLUSTER.bootstrapServers)
    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, APP_ID + "-result-consumer")
    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    RESULT_CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MetricTankSerde.deserializer().getClass)

    STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, EmbeddedKafka.CLUSTER.bootstrapServers)
    STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID)
    STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
    STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "300")
    STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams")

    IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG)

    CHANGELOG_TOPIC = s"$APP_ID-AggregatedMetricPointStore-changelog"
  }

  override def afterEach(): Unit = {
    EmbeddedKafka.CLUSTER.deleteTopic(INPUT_TOPIC)
    EmbeddedKafka.CLUSTER.deleteTopic(OUTPUT_TOPIC)
  }

  def currentTimeInSecs: Long = {
    System.currentTimeMillis() / 1000l
  }

  protected def produceMetricPointsAsync(maxMetricPoints: Int,
                                         produceInterval: FiniteDuration,
                                         metricName: String,
                                         totalIntervalInSecs: Long = PUNCTUATE_INTERVAL_SEC
                                        ): Unit = {
    var epochTimeInSecs = 0l
    var idx = 0
    scheduler.scheduleWithFixedDelay(() => {
      if (idx < maxMetricPoints) {
        val metricPoint = randomMetricPoint(metricName = metricName, timestamp = epochTimeInSecs)
        val keyValue = List(new KeyValue[String, MetricPoint](metricPoint.getMetricPointKey(true), metricPoint)).asJava
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
          INPUT_TOPIC,
          keyValue,
          PRODUCER_CONFIG,
          epochTimeInSecs)
        epochTimeInSecs = epochTimeInSecs + (totalIntervalInSecs / (maxMetricPoints - 1))
      }
      idx = idx + 1
    }, 0, produceInterval.toMillis, TimeUnit.MILLISECONDS)
  }

  def randomMetricPoint(metricName: String,
                        value: Long = Math.abs(Random.nextInt()),
                        timestamp: Long = currentTimeInSecs): MetricPoint = {
    MetricPoint(metricName, MetricType.Gauge, Map[String, String](), value, timestamp)
  }


}
