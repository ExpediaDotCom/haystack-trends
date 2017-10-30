package com.expedia.www.haystack.trends.feature.tests.serde

import com.expedia.open.tracing.Span
import com.expedia.www.haystack.trends.feature.FeatureSpec
import com.expedia.www.haystack.trends.serde.SpanSerde

class SpanSerdeSpec extends FeatureSpec {

  feature("serialize and deserialize span objects in protobuf binary format") {

    val TOPIC_NAME = "dummy"

    scenario("serializing valid span object") {

      Given("a valid span object")
      val duration = System.currentTimeMillis
      val span = generateTestSpan(duration)

      When("its serialized")
      val bytes = SpanSerde.serializer.serialize(TOPIC_NAME, span)

      Then("it should be serialized in protobuf binary format")
      bytes should not be null
      Span.parseFrom(bytes) shouldBe span


    }

    scenario("deserializing valid span object") {

      Given("a byte array of a serialized span object")
      val duration = System.currentTimeMillis
      val span = generateTestSpan(duration)
      val bytes = SpanSerde.serializer.serialize(TOPIC_NAME, span)


      When("its deserialized")

      val deserializedSpan = SpanSerde.deserializer.deserialize(TOPIC_NAME, bytes)

      Then("it should return the same span object as the one before serializing")
      span shouldBe deserializedSpan

    }
    scenario("deserializing invalid span object") {

      Given("a invalid byte array")
      val bytes = "Random String".getBytes()


      When("its deserialized")

      val deserializedSpan = SpanSerde.deserializer.deserialize(TOPIC_NAME, bytes)

      Then("it should return a null")
      deserializedSpan shouldBe null
    }
  }
}
