package com.expedia.www.haystack.metricpoints.feature

import com.expedia.www.haystack.metricpoints.health.{HealthController, UpdateHealthStatusFile}
import org.scalatest.{FunSpec, Matchers}

class HealthControllerSpec extends FunSpec with Matchers {

  private val statusFile = "/tmp/app-health.status"

  describe("file based health checker") {
    it("should set the state as healthy if previous state is not set or unhealthy") {
      val healthChecker = HealthController
      healthChecker.addListener(new UpdateHealthStatusFile(statusFile))
      healthChecker.isHealthy shouldBe false
      healthChecker.setHealthy()
      healthChecker.isHealthy shouldBe true
      readStatusLine shouldEqual "healthy"
    }

    it("should set the state as unhealthy if previous state is healthy") {
      val healthChecker = HealthController
      healthChecker.addListener(new UpdateHealthStatusFile(statusFile))

      healthChecker.setHealthy()
      healthChecker.isHealthy shouldBe true
      readStatusLine shouldEqual "healthy"

      healthChecker.setUnhealthy()
      healthChecker.isHealthy shouldBe false
      readStatusLine shouldEqual "unhealthy"
    }
  }

  private def readStatusLine = io.Source.fromFile(statusFile).getLines().toList.head
}
