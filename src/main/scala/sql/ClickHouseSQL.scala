package com.github.oliverdding.hpient
package sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.Filter

import java.time.ZoneId

private class ClickHouseSQL extends SQLHelper {

  private lazy val sql = new StringBuilder("SELECT ")

  def select(fields: Seq[String]): ClickHouseSQL = {
    this.sql.append(_select(fields))
    this
  }

  def selectAll(): ClickHouseSQL = {
    this.sql.append("*")
    this
  }

  def from(namespace: String, table: String): ClickHouseSQL = {
    this.sql.append(s" FROM ${quoted(namespace)}.${quoted(table)} ")
    this
  }

  def where(filters: Seq[Filter])(implicit tz: ZoneId): ClickHouseSQL = {
    this.sql.append("WHERE ")
    this.sql.append(s"${_where(filters)(tz)}")
    this
  }

  override def toString: String = sql.toString()
}

object ClickHouseSQL {
  class Builder extends Logging {

    private var isSelectAll: Boolean = false
    private var fields: Option[Seq[String]] = None
    private var filters: Option[Seq[Filter]] = None
    private var namespace: Option[String] = None
    private var table: Option[String] = None

    def selectAll(): Unit =
      isSelectAll = true

    def select(fields: String*): Builder = {
      this.fields = Some(fields)
      this
    }

    def where(filters: Filter*): Builder = {
      this.filters = Some(filters)
      this
    }

    def from(namespace: String, table: String): Builder = {
      this.namespace = Some(namespace)
      this.table = Some(table)
      this
    }

    private def fulfill(): Boolean = {
      if (!isSelectAll) {
        if (fields.isEmpty) {
          logError("fields is empty")
          return false
        }
      }
      if (namespace.isEmpty) {
        logError("namespace is empty")
        return false
      }
      if (table.isEmpty) {
        logError("table is empty")
        return false
      }
      true
    }

    def build(): String =
      build(ZoneId.systemDefault())

    /**
     * Build the sql string from builder.
     *
     * Examples:
     * scala
     * ClickHouseSQL().select("name", "age", "born").from("home", "list").where( EqualTo("name",
     * "oliverdding"),GreaterThan("age", 30), LessThan("born", Instant.now()) ).build(ZoneId.of("UTC+8"))
     *
     * @param tz
     *   Timezone that used to format java.time.Instant.
     * @return
     *   sql string
     */
    def build(implicit tz: ZoneId): String = {
      if (!fulfill()) {
        throw new Exception("cannot construct sql because lack of parameters")
      }
      val sql = new ClickHouseSQL()
      if (isSelectAll) {
        sql.selectAll().from(namespace.get, table.get)
      } else {
        sql.select(fields.get).from(namespace.get, table.get)
      }
      if (filters.isDefined) {
        sql.where(filters.get)(tz)
      }
      val res = sql.toString
      logDebug(s"generate sql:\n$res")
      res
    }
  }

  def apply(): ClickHouseSQL.Builder =
    new Builder()
}
