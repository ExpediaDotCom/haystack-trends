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
package com.expedia.www.haystack.trends.config

import java.util.Properties

import com.expedia.www.haystack.commons.config.ConfigurationLoader
import com.expedia.www.haystack.commons.entities.encoders.{Encoder, EncoderFactory}
import com.expedia.www.haystack.commons.kstreams.MetricPointTimestampExtractor
import com.expedia.www.haystack.commons.kstreams.serde.metricpoint.MetricPointSerializer
import com.expedia.www.haystack.trends.config.entities.{KafkaConfiguration, KafkaProduceConfiguration}
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerConfig.{KEY_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS_CONFIG}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.processor.TimestampExtractor

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

class AppConfiguration {
  private val config = ConfigurationLoader.loadConfigFileWithEnvOverrides()

  val healthStatusFilePath: String = config.getString("health.status.path")
  private val kafka = config.getConfig("kafka")
  private val producerConfig = kafka.getConfig("producer")
  private val consumerConfig = kafka.getConfig("consumer")
  private val streamsConfig = kafka.getConfig("streams")

  /**
    *
    * @return delay in logging to state store
    */
  def loggingDelayInSeconds: Long = {
    config.getLong("statestore.logging.delay.seconds")
  }

  /**
    *
    * @return whether logging for state store is enabled
    */
  def enableStateStoreLogging: Boolean = {
    config.getBoolean("statestore.enable.logging")
  }

  /**
    *
    * @return type of encoder to use on metricpoint key names
    */
  def encoder: Encoder = {
    val encoderType = config.getString("metricpoint.encoder.type")
    EncoderFactory.newInstance(encoderType)
  }

  /**
    *
    * @return max allowable value for a histogram metric
    */
  def histogramMaxValue: Int = {
    config.getInt("histogram.max.value")
  }

  /**
    *
    * @return allowable precision of histogram must be 0 <= value <= 5
    */
  def histogramPrecision: Int = {
    config.getInt("histogram.precision")
  }

  /**
    *
    * @return whether period in metric point service & operation name needs to be replaced
    */
  def stateStoreCacheSize: Int = {
    config.getInt("statestore.cache.size")
  }


  /**
    *
    * @return state store stream config while aggregating
    */
  def stateStoreConfig: Map[String, String] = {
    val stateStoreConfigs = config.getConfig("state.store")
    if (stateStoreConfigs.isEmpty) {
      new HashMap[String, String]
    }
    stateStoreConfigs.entrySet().asScala.map(entry => entry.getKey -> entry.getValue.unwrapped().toString).toMap
  }

  /**
    *
    * @return streams configuration object
    */
  def kafkaConfig: KafkaConfiguration = {

    // verify if the applicationId and bootstrap server config are non empty
    def verifyRequiredProps(props: Properties): Unit = {
      require(props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG).nonEmpty)
      require(props.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG).nonEmpty)
    }

    def addProps(config: Config, props: Properties, prefix: (String) => String = identity): Unit = {
      config.entrySet().asScala.foreach(kv => {
        val propKeyName = prefix(kv.getKey)
        props.setProperty(propKeyName, kv.getValue.unwrapped().toString)
      })
    }

    def getExternalKafkaProps(producerConfig: Config): Option[Properties] = {

      if (producerConfig.getBoolean("enable.external.kafka.produce")) {
        val props = new Properties()
        val kafkaProducerProps = producerConfig.getConfig("props")

        kafkaProducerProps.entrySet() forEach {
          kv => {
            props.setProperty(kv.getKey, kv.getValue.unwrapped().toString)
          }
        }

        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, classOf[MetricPointSerializer].getCanonicalName)

        require(props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).nonEmpty)
        Option(props)
      } else {
        Option.empty
      }
    }

    /**
      *
      * @return returns the kafka autoreset configuration
      */
    def getKafkaAutoReset: AutoOffsetReset = {
      if (streamsConfig.hasPath("auto.offset.reset")) AutoOffsetReset.valueOf(streamsConfig.getString("auto.offset.reset").toUpperCase)
      else AutoOffsetReset.LATEST
    }


    val props = new Properties
    // add stream specific properties
    addProps(streamsConfig, props)
    // validate props
    verifyRequiredProps(props)

    val timestampExtractor = Option(props.getProperty("timestamp.extractor")) match {
      case Some(timeStampExtractorClass) =>
        Class.forName(timeStampExtractorClass).newInstance().asInstanceOf[TimestampExtractor]
      case None =>
        new MetricPointTimestampExtractor
    }

    //set timestamp extractor
    props.setProperty("timestamp.extractor", timestampExtractor.getClass.getName)

    KafkaConfiguration(
      new StreamsConfig(props),
      producerConfig = KafkaProduceConfiguration(producerConfig.getString("topic"), getExternalKafkaProps(producerConfig), producerConfig.getBoolean("enable.external.kafka.produce")),
      consumeTopic = consumerConfig.getString("topic"),
      getKafkaAutoReset,
      timestampExtractor,
      kafka.getLong("close.timeout.ms"))
  }

}

object AppConfiguration extends AppConfiguration
