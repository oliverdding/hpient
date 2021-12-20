package com.github.oliverdding.hpient
package sql

import org.apache.spark.sql.sources.{EqualTo, GreaterThan, LessThan}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp
import java.time.{Instant, ZoneId}

class ClickHouseSQLTest extends AnyFunSuite {
  test("select without filter") {
    info(ClickHouseSQL().select("name", "support_filters").from("system", "table_engines").build())
  }

  test("select some fields") {
    info(ClickHouseSQL().select("name", "age", "time").from("system", "table_engines").where(
      EqualTo("name", "SQLite")
    ).build())
  }

  test("multiply filters") {
    info(ClickHouseSQL().select("name", "age", "born").from("home", "list").where(
      EqualTo("name", "oliverdding"),
      GreaterThan("age", 30),
      LessThan("born", Timestamp.valueOf("2000-01-01 00:00:01"))
    ).build())
  }

  test("different timezone") {
    info(ClickHouseSQL(ZoneId.of("UTC+8")).select("name", "age", "born").from("home", "list").where(
      EqualTo("name", "oliverdding"),
      GreaterThan("age", 30),
      LessThan("born", Instant.now())
    ).build())
  }
}
