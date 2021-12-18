package com.github.oliverdding.hpient

import org.scalatest.funsuite.AnyFunSuite

import sttp.client3._
import sttp.model.StatusCode

class SimpleClientTest extends AnyFunSuite {

  test("ping") {
    val myRequest = basicRequest
      .get(uri"http://dev:8123/ping")
      .header("Connection", "Close")
      .header("Content-Type", "text/plain")
      .auth
      .basic("default", "")

    val backend = HttpURLConnectionBackend()
    val response = myRequest.send(backend)

    assert(response.body == Right("Ok.\n"))
    backend.close()
  }

  test("select") {
    val myRequest = basicRequest
      .post(uri"http://dev:8123/?compress=1")
      .header("Connection", "Close")
      .header("Content-Type", "text/plain")
      .auth
      .basic("default", "")
      .body("SELECT * FROM system.table_engines")

    val backend = HttpURLConnectionBackend()
    val response = myRequest.send(backend)

    assert(response.code == StatusCode.Ok)
    backend.close()
  }

  test("arrow") {
    val myRequest = basicRequest
      .post(uri"http://dev:8123/?compress=1")
      .header("Connection", "Close")
      .header("Content-Type", "text/plain")
      .auth
      .basic("default", "")
      .body("SELECT * FROM system.table_engines FORMAT ArrowStream")

    val backend = HttpURLConnectionBackend()
    val response = myRequest.send(backend)

    assert(response.code == StatusCode.Ok)
    backend.close()
  }

}
