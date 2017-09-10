package com.expedia.www.haystack.metricpoints.health

import com.expedia.www.haystack.metricpoints.health.HealthStatus.HealthStatus

trait HealthStatusChangeListener {

  def onChange(status: HealthStatus): Unit
}
