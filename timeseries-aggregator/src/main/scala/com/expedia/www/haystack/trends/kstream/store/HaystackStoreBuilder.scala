package org.apache.kafka.streams.state.internals

import java.util

import com.expedia.www.haystack.trends.aggregation.TrendMetric
import com.expedia.www.haystack.trends.kstream.serde.TrendMetricSerde
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.state.internals.{InMemoryKeyValueLoggedStore, InMemoryLRUCacheStoreSupplier, MemoryNavigableLRUCache}
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder}

import scala.collection.JavaConverters._
import scala.collection.mutable


class HaystackStoreBuilder(storeName: String, maxCacheSize: Int) extends StoreBuilder[KeyValueStore[String, TrendMetric]] {

  private var changeLogEnabled = false
  private var changeLogProperties = mutable.Map[String, String]()

  override def loggingEnabled(): Boolean = {
    changeLogEnabled
  }

  override def withLoggingEnabled(config: util.Map[String, String]): StoreBuilder[KeyValueStore[String, TrendMetric]] = {
    changeLogEnabled = true
    changeLogProperties = config.asScala
    this
  }

  override def logConfig(): util.Map[String, String] = changeLogProperties.asJava

  override def name(): String = {
    storeName
  }

  override def withCachingEnabled(): StoreBuilder[KeyValueStore[String, TrendMetric]] = {
    changeLogEnabled = true
    this
  }

  override def build(): KeyValueStore[String, TrendMetric] = {
    if (changeLogEnabled) {
      new InMemoryKeyValueLoggedStore[String, TrendMetric](new MemoryNavigableLRUCache(storeName, maxCacheSize, new StringSerde, TrendMetricSerde), new StringSerde, TrendMetricSerde)
    }
    else {
      new MemoryNavigableLRUCache(storeName, maxCacheSize, new StringSerde, TrendMetricSerde)
    }
  }



  override def withLoggingDisabled(): StoreBuilder[KeyValueStore[String, TrendMetric]] = {
    changeLogEnabled = false
    changeLogProperties.clear()
    this
  }
}
