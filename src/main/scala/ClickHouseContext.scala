package com.github.oliverdding.hpient

case class ClickHouseContext(
  namespace: String,
  table: String,
  host: String,
  port: Int,
  username: String,
  password: String
)
