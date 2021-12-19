package com.github.oliverdding.hpient

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.internal.Logging

import java.time.format.DateTimeFormatter

object Utils extends Logging {

  @transient lazy val dateFmt: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd")
  @transient lazy val dateTimeFmt: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  @transient lazy val legacyDateFmt: FastDateFormat =
    FastDateFormat.getInstance("yyyy-MM-dd")
  @transient lazy val legacyDateTimeFmt: FastDateFormat =
    FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
}
