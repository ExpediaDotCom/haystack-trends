package com.expedia.www.haystack.trends.entities

case class HaystackTrend(keys: Map[String, String], value: Long, timestamp: Long) {
  def getTrendKey: String = {
    keys.foldLeft("")((trendKey, tuple) => {
      trendKey + s"${tuple._1}->${tuple._2}|"
    })
  }
}

object TrendKeys {
  val OPERATION_NAME_KEY = "operationName"
  val SERVICE_NAME_KEY = "serviceName"
  val METRIC_TYPE_KEY = "metricType"
  val ERROR_KEY = "error"
  val METRIC_FIELD_KEY = "metricField"

  object TrendMetric extends Enumeration {
    type TrendMetricType = Value
    val HISTOGRAM, COUNT = Value
  }

}


