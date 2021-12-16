package com.github.oliverdding.hpient

import org.scalatest.funsuite.AnyFunSuite

import scala.util.Using
import sttp.client3._
import sttp.model.StatusCode

class SimpleClientTest extends AnyFunSuite {

  test("ping") {
    val myRequest = basicRequest
      .auth.basic("default", "")
      .get(uri"http://dev:8123/ping")

    val backend = HttpURLConnectionBackend()
    val response = myRequest.send(backend)

    assert(response.body == Right("Ok.\n"))
    backend.close()
  }

  test("select") {
    val myRequest = basicRequest
      .auth.basic("default", "")
      .body("SELECT * FROM system.table_engines")
      .post(uri"http://dev:8123/?compress=1")
    val backend = HttpURLConnectionBackend()
    val response = myRequest.send(backend)

    assert(response.code == StatusCode.Ok)
    backend.close()
  }

  test("arrow") {
    val myRequest = basicRequest
      .auth.basic("default", "")
      .body("SELECT * FROM system.table_engines FORMAT ArrowStream")
      .post(uri"http://dev:8123/?compress=1")
    val backend = HttpURLConnectionBackend()
    val response = myRequest.send(backend)

    assert(response.code == StatusCode.Ok)
    backend.close()
  }

}
