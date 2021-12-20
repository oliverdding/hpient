package com.github.oliverdding.hpient

import org.scalatest.funsuite.AnyFunSuite

class ClickHouseLetTest extends AnyFunSuite {

  test("testSchema") {
    implicit val context = new ClickHouseContext("tencent_public", "singleton", "159.75.36.118", 5448, "default", "")
    println(ClickHouseLet.schema())
  }

}
