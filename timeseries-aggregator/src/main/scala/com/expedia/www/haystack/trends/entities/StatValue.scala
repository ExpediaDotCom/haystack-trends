package com.expedia.www.haystack.trends.entities

object StatValue extends Enumeration {
  type StatValue = Value

  val MEAN = Value("mean")
  val MAX = Value("max")
  val MIN = Value("min")
  val COUNT = Value("count")
  val STDDEV = Value("std")
  val PERCENTILE_99 = Value("*_99")
  val MEDIAN = Value("*_50")

}
