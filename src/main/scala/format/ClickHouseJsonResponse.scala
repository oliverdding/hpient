package com.github.oliverdding.hpient
package format

case class ClickHouseJsonResponse (meta: List[ClickHouseJsonField])

case class ClickHouseJsonField (name: String, `type`: String)