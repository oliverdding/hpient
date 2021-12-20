package com.github.oliverdding.hpient

abstract class ClickHouseException(code: Int, reason: String) extends RuntimeException(s"[$code] $reason")

case class ClickHouseClientException(code: Int, reason: String) extends ClickHouseException(code, reason)

case class ClickHouseQueryException(code: Int, reason: String) extends ClickHouseException(code, reason)

object ClickHouseStatusCode extends Enumeration {
  type ClickhouseStatusCode = Value
  val UNKNOWN_ERROR: ClickHouseStatusCode.Value = Value(-2, "UNKNOWN_ERROR")
  val CLIENT_ERROR: ClickHouseStatusCode.Value = Value(-1, "CLIENT_ERROR")

}
