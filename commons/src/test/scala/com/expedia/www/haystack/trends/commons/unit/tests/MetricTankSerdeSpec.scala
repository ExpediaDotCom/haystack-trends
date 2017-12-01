package com.expedia.www.haystack.trends.commons.unit.tests

import com.expedia.www.haystack.trends.commons.entities.{MetricPoint, MetricType, TagKeys}
import com.expedia.www.haystack.trends.commons.serde.metricpoint.MetricTankSerde
import com.expedia.www.haystack.trends.commons.unit.UnitTestSpec
import org.msgpack.core.MessagePack

class MetricTankSerdeSpec extends UnitTestSpec {
  val statusFile = "/tmp/app-health.status"
  val DURATION_METRIC_NAME = "duration"
  val SERVICE_NAME = "dummy_service"
  val OPERATION_NAME = "dummy_operation"
  val TOPIC_NAME = "dummy"


  val metricTags = Map(TagKeys.OPERATION_NAME_KEY -> OPERATION_NAME,
    TagKeys.SERVICE_NAME_KEY -> SERVICE_NAME)

  "MetricTank serde for metric point" should {

    "serialize and deserialize metric points using messagepack" in {

      Given("metric point")
      val metricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, metricTags, 80, currentTimeInSecs)

      When("its serialized using the metricTank Serde")
      val serializedBytes = MetricTankSerde.serializer().serialize(TOPIC_NAME, metricPoint)

      Then("it should be encoded as message pack")
      val unpacker = MessagePack.newDefaultUnpacker(serializedBytes)
      unpacker should not be null
    }


    "serialize and deserialize simple metric points without loosing data" in {

      Given("metric point")
      val metricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, metricTags, 80, currentTimeInSecs)

      When("its serialized in the metricTank Format")
      val serializedBytes = MetricTankSerde.serializer().serialize(TOPIC_NAME, metricPoint)
      val deserializedMetricPoint = MetricTankSerde.deserializer().deserialize(TOPIC_NAME, serializedBytes)

      Then("it should be encoded as message pack")
      metricPoint shouldEqual deserializedMetricPoint
    }

    "serialize and deserialize metric points with tag values containing special characters without loosing data" in {

      val tagWithSpecialCharacters = Map(TagKeys.SERVICE_NAME_KEY -> SERVICE_NAME, TagKeys.OPERATION_NAME_KEY -> "service:someOp")

      Given("metric point")
      val metricPoint = MetricPoint(DURATION_METRIC_NAME, MetricType.Gauge, tagWithSpecialCharacters, 80, currentTimeInSecs)

      When("its serialized in the metricTank Format")
      val serializedBytes = MetricTankSerde.serializer().serialize(TOPIC_NAME, metricPoint)
      val deserializedMetricPoint = MetricTankSerde.deserializer().deserialize(TOPIC_NAME, serializedBytes)

      Then("it should be encoded as message pack")
      metricPoint shouldEqual deserializedMetricPoint
    }
  }

  private def readStatusLine = io.Source.fromFile(statusFile).getLines().toList.head
}
