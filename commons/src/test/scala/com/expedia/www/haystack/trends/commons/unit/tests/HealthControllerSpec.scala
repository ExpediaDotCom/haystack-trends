package com.expedia.www.haystack.trends.commons.unit.tests

import com.expedia.www.haystack.trends.commons.health.{HealthController, UpdateHealthStatusFile}
import com.expedia.www.haystack.trends.commons.unit.UnitTestSpec

class HealthControllerSpec extends UnitTestSpec {
  val statusFile = "/tmp/app-health.status"


  "file based health checker" should {

    "set the value with the correct boolean value for the app's health status" in {
      Given("a file path")

      When("checked with default state")
      val healthChecker = HealthController
      healthChecker.addListener(new UpdateHealthStatusFile(statusFile))
      val status = healthChecker.isHealthy

      Then("default state should be unhealthy")
      status shouldBe false

      When("explicitly set as healthy")
      healthChecker.setHealthy()

      Then("The state should be updated to healthy")
      healthChecker.isHealthy shouldBe true
      readStatusLine shouldEqual "true"

      When("explicitly set as unhealthy")
      healthChecker.setUnhealthy()

      Then("The state should be updated to unhealthy")
      healthChecker.isHealthy shouldBe false
      readStatusLine shouldBe "false"
    }
  }

  private def readStatusLine = io.Source.fromFile(statusFile).getLines().toList.head
}
