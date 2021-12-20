package com.github.oliverdding.hpient

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowStreamReader, SeekableReadChannel}
import org.apache.commons.compress.utils.{IOUtils, SeekableInMemoryByteChannel}
import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.scalatest.funsuite.AnyFunSuite
import sttp.client3._
import sttp.model.StatusCode

import java.io.{ByteArrayInputStream, IOException}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IteratorHasAsScala}
import scala.util.Using

class ArrowStreamTest extends AnyFunSuite {

  BasicConfigurator.configure()

  Logger.getRootLogger.setLevel(Level.TRACE)

  test("arrow stream from clickhouse") {
    val myRequest = basicRequest
      .post(uri"http://159.75.36.118:5448/")
      .header("Connection", "Close")
      .header("Content-Type", "text/plain")
      .auth
      .basic("default", "")
      .response(asByteArray)
      .body(
        "SELECT * FROM tencent_public.singleton_dist WHERE category = 'PERF_CRASH' and data_time >= '2021-12-20 20:00:00' LIMIT 1 FORMAT ArrowStream"
      )

    val backend = HttpURLConnectionBackend()
    val response = myRequest.send(backend)
    assert(response.code == StatusCode.Ok)
    val body = response.body match {
      case Left(x) => fail(s"HTTP return error: $x")
      case Right(x) => x
    }

    throw new Exception("test")

    Using(new RootAllocator(Long.MaxValue)) { allocator =>
      Using(new ArrowStreamReader(new ByteArrayInputStream(body), allocator)) {
        streamReader =>
          Using(streamReader.getVectorSchemaRoot) { schemaRoot =>
            println(schemaRoot.getSchema)
            while (streamReader.loadNextBatch()) {
              val fieldVectorItr = schemaRoot.getFieldVectors.iterator()
              val sparkVectors = fieldVectorItr.asScala
                .map[ColumnVector] { fieldVector =>
                  new ArrowColumnVector(fieldVector)
                }
                .toArray
              println("TTTTTTTT")
              Using(new ColumnarBatch(sparkVectors, schemaRoot.getRowCount)) {
                columnarBatch =>
                  println("Got it --->")
                  println(
                    s"rows: ${columnarBatch.numRows()}; cols: ${columnarBatch.numCols()}"
                  )
              }
              println("EEEEEEEEE")
            }
          }
      }
    }

    backend.close()
  }

  test("arrow from file") {
    Using(getClass.getResourceAsStream("/table_engines.arrow")) {
      arrowFileStream =>
        Using(
          new SeekableInMemoryByteChannel(IOUtils.toByteArray(arrowFileStream))
        ) { channel =>
          val seekableReadChannel = new SeekableReadChannel(channel)
          Using(
            new ArrowFileReader(
              seekableReadChannel,
              new RootAllocator(Integer.MAX_VALUE)
            )
          ) { arrowFileReader =>
            val root = arrowFileReader.getVectorSchemaRoot
            println(s"schema is ${root.getSchema}")

            val arrowBlocks = arrowFileReader.getRecordBlocks
            println(s"num of arrow blocks is ${arrowBlocks.size()}")
            println(s"num of col: ${root.getFieldVectors.size()}")

            arrowBlocks.asScala.foreach { arrowBlock =>
              if (!arrowFileReader.loadRecordBatch(arrowBlock)) {
                throw new IOException("Expected to read record batch")
              }
              println(s"num of row in this block: ${root.getRowCount}")
              val fieldVectorItr = root.getFieldVectors.iterator()
              val sparkVectors = fieldVectorItr.asScala
                .map[ColumnVector] { fieldVector =>
                  new ArrowColumnVector(fieldVector)
                }
                .toArray
              val batch = new ColumnarBatch(sparkVectors, root.getRowCount)
              println(batch)
            }
          }
        }
    }
  }
}
