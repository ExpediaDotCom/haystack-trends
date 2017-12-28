package com.expedia.www.haystack.trends.feature.tests.kstreams

import com.expedia.www.haystack.trends.commons.entities.{MetricPoint, MetricType}
import com.expedia.www.haystack.trends.feature.FeatureSpec
import com.expedia.www.haystack.trends.kstream.MetricPointTimestampExtractor
import org.apache.kafka.clients.consumer.ConsumerRecord

class MetricPointTimestampExtractorSpec extends FeatureSpec {

  feature("MetricPointTimestampExtractor should extract timestamp from MetricPoint") {

    scenario("a valid MetricPoint") {

      Given("a metric point with some timestamp")
      val currentTimeInSecs = System.currentTimeMillis()
      val metricPoint = MetricPoint("duration", MetricType.Gauge, null, 80, currentTimeInSecs)
      val metricPointTimestampExtractor = new MetricPointTimestampExtractor
      val record: ConsumerRecord[AnyRef, AnyRef] = new ConsumerRecord("dummy-topic", 1, 1, "dummy-key", metricPoint)

      When("extract timestamp")
      val epochTime = metricPointTimestampExtractor.extract(record, System.currentTimeMillis())

      Then("extracted time should equal metric point time")
      epochTime shouldEqual(currentTimeInSecs)
    }
  }
}
