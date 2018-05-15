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

import java.util.function.Supplier

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.commons.entities.MetricPoint
import com.expedia.www.haystack.commons.kstreams.serde.SpanSerde
import com.expedia.www.haystack.commons.kstreams.serde.metricpoint.MetricTankSerde
import com.expedia.www.haystack.trends.config.entities.{KafkaConfiguration, TransformerConfiguration}
import com.expedia.www.haystack.trends.transformer.MetricPointTransformer
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.Produced

import scala.collection.JavaConverters._

class Streams(kafkaConfig: KafkaConfiguration, transformerConfiguration: TransformerConfiguration) extends Supplier[Topology]
  with MetricPointGenerator {


  private[trends] def initialize(builder: StreamsBuilder): Topology = {
    builder.stream(kafkaConfig.consumeTopic, Consumed.`with`(kafkaConfig.autoOffsetReset).withKeySerde(new StringSerde).withValueSerde(new SpanSerde).withTimestampExtractor(kafkaConfig.timestampExtractor))
      .flatMap[String, MetricPoint] {
      (_: String, span: Span) => mapToMetricPointKeyValue(span)
    }.to(kafkaConfig.produceTopic, Produced.`with`(new StringSerde(), new MetricTankSerde(transformerConfiguration.encoder)))
    builder.build()
  }

  private def mapToMetricPointKeyValue(span: Span): java.util.List[KeyValue[String, MetricPoint]] = {
    generateMetricPoints(transformerConfiguration.blacklistedServices)(MetricPointTransformer.allTransformers)(span, transformerConfiguration.enableMetricPointServiceLevelGeneration)
      .map {
        metricPoint => new KeyValue(metricPoint.getMetricPointKey(transformerConfiguration.encoder), metricPoint)
      }.asJava
  }


  override def get(): Topology = {
    val builder = new StreamsBuilder()
    initialize(builder)
  }
}
