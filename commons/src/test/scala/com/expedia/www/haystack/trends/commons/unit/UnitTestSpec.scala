package com.expedia.www.haystack.trends.commons.unit

import org.scalatest._

protected trait UnitTestSpec extends WordSpec with GivenWhenThen with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

 protected def currentTimeInSecs: Long = {
    System.currentTimeMillis() / 1000l
  }
}
