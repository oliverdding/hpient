package com.github.oliverdding.hpient

import format.{ClickHouseJsonResponse, SchemaUtils}

import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import sttp.client3.{asByteArray, basicRequest, HttpURLConnectionBackend, Identity, SttpBackend, UriContext}

/**
 * Higher API that used to query schema or data
 */
object ClickHouseLet extends Logging {

  implicit val codec: JsonValueCodec[ClickHouseJsonResponse] = JsonCodecMaker.make

  def schema(): StructType = {
    val backend: SttpBackend[Identity, Any] =
      HttpURLConnectionBackend()
    val response = basicRequest
      .body("SELECT * FROM tencent_public.singleton_dist WHERE 1=0 FORMAT JSON")
      .post(uri"http://159.75.36.118:5448/")
      .auth
      .basic("default", "")
      .response(asByteArray.getRight)
      .send(backend)
    val res = readFromArray(response.body)
    SchemaUtils.fromClickHouseSchema(res.meta.map { m =>
      (m.name, m.`type`)
    })
  }
}
