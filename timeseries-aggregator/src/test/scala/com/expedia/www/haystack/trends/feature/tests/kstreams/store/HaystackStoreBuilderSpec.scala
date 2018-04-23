package com.expedia.www.haystack.trends.feature.tests.kstreams.store

import com.expedia.www.haystack.trends.aggregation.TrendMetric
import com.expedia.www.haystack.trends.feature.FeatureSpec
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.internals.{HaystackStoreBuilder, InMemoryKeyValueLoggedStore, MemoryNavigableLRUCache}

class HaystackStoreBuilderSpec extends FeatureSpec {

  feature("Haystack Store Builder should build appropriate store for haystack metrics") {

    scenario("build store with changelog enabled") {

      Given("a haystack store builder")
      val storeName = "test-store"
      val cacheSize = 100
      val storeBuilder = new HaystackStoreBuilder(storeName, cacheSize)

      When("change logging is enabled")
      storeBuilder.withCachingEnabled()
      val store = storeBuilder.build()

      Then("it should build a lru-cache based changelogging store")
      store shouldBe a [InMemoryKeyValueLoggedStore[String, TrendMetric]]
    }

    scenario("build store with changelog disabled") {

      Given("a haystack store builder")
      val storeName = "test-store"
      val cacheSize = 100
      val storeBuilder = new HaystackStoreBuilder(storeName, cacheSize)

      When("change logging is disabled")
      storeBuilder.withLoggingDisabled()
      val store = storeBuilder.build()

      Then("it should build a lru-cache store")
      store shouldBe a [MemoryNavigableLRUCache[String, TrendMetric]]
    }
  }
}
