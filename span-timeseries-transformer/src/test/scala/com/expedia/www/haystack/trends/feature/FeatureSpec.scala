/*
 *  Copyright 2017 Expedia, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.expedia.www.haystack.trends.feature

import com.expedia.open.tracing.Span
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FeatureSpecLike, GivenWhenThen, Matchers}


trait FeatureSpec extends FeatureSpecLike with GivenWhenThen with Matchers with EasyMockSugar {
  def generateTestSpan(duration: Long): Span = {
    val operationName = "testSpan"
    val serviceName = "testService"
    Span.newBuilder()
      .setDuration(duration)
      .setOperationName(operationName)
      .setServiceName(serviceName)
      .build()
  }
}
