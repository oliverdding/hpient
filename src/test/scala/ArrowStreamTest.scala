package com.github.oliverdding.hpient

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.scalatest.funsuite.AnyFunSuite
import sttp.client3._
import sttp.model.StatusCode

import java.io.ByteArrayInputStream
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Using

class ArrowStreamTest extends AnyFunSuite {

  BasicConfigurator.configure()

  Logger.getRootLogger.setLevel(Level.TRACE)

  test("arrow") {
    val myRequest = basicRequest
      .post(uri"http://dev:8123/")
      .header("Connection", "Close")
      .header("Content-Type", "text/plain")
      .auth
      .basic("default", "")
      .response(asByteArray)
      .body("SELECT * FROM system.table_engines FORMAT ArrowStream")

    val backend = HttpURLConnectionBackend()
    val response = myRequest.send(backend)
    assert(response.code == StatusCode.Ok)
    val body = response.body match {
      case Left(x)  => fail(s"HTTP return error: $x")
      case Right(x) => x
    }

    Using(new RootAllocator(Long.MaxValue)) { allocator =>
      Using(new ArrowStreamReader(new ByteArrayInputStream(body), allocator)) {
        streamReader =>
          while (streamReader.loadNextBatch()) {
            Using(streamReader.getVectorSchemaRoot) { schemaRoot =>
              println(schemaRoot.getSchema)
              val fieldVectorItr = schemaRoot.getFieldVectors.iterator()
              val sparkVectors = fieldVectorItr.asScala
                .map[ColumnVector] { fieldVector =>
                  new ArrowColumnVector(fieldVector)
                }
                .toArray
              Using(new ColumnarBatch(sparkVectors, schemaRoot.getRowCount)) {
                columnarBatch =>
                  println("Got it --->")
                  println(
                    s"rows: ${columnarBatch.numRows()}; cols: ${columnarBatch.numCols()}"
                  )
              }
            }
          }
      }
    }

    backend.close()
  }
}
