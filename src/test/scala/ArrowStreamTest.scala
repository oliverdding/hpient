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

class ArrowStreamTest extends AnyFunSuite {

  BasicConfigurator.configure()

  Logger.getRootLogger.setLevel(Level.WARN)

  test("arrow stream from clickhouse") {
    val myRequest = basicRequest
      .post(uri"http://159.75.36.118:5448/?buffer_size=1024&wait_end_of_query=1")
      .header("Connection", "Close")
      .header("Content-Type", "text/plain")
      .auth
      .basic("default", "")
      .response(asByteArray)
      .body(
        "SELECT app_id, platform, id, user_id, device_id, category, arch, os_ver, ip, region, version model FROM tencent_public.singleton_dist WHERE category = 'PERF_CRASH' and data_time >= '2021-12-20 20:00:00' LIMIT 100000 FORMAT ArrowStream"
      )

    val backend = HttpURLConnectionBackend()
    val response = myRequest.send(backend)
    assert(response.code == StatusCode.Ok)
    val body = response.body match {
      case Left(x) => fail(s"HTTP return error: $x")
      case Right(x) => x
    }

    val allocator = new RootAllocator(Integer.MAX_VALUE)
    val arrowStreamReader = new ArrowStreamReader(
      new ByteArrayInputStream(body),
      allocator
    )

    val root = arrowStreamReader.getVectorSchemaRoot
    println(s"schema is ${root.getSchema}")
    println(s"bytes read: ${arrowStreamReader.bytesRead()}")

    while (arrowStreamReader.loadNextBatch()) {
      println("-------------------------------------")
      println(s"num of row in this block: ${root.getRowCount}")
      println(s"bytes read: ${arrowStreamReader.bytesRead()}")
      val vectors = root.getFieldVectors.asScala
      val fieldVectorItr = vectors.iterator
      val sparkVectors = fieldVectorItr.map[ColumnVector] { vector =>
        new ArrowColumnVector(vector)
      }.toArray
      val batch = new ColumnarBatch(sparkVectors, root.getRowCount)
      println(s"batch has cols: ${batch.numCols()}")
      println(s"batch has rows: ${batch.numRows()}")
    }

    backend.close()
  }

  test("arrow from clickhouse") {
    val myRequest = basicRequest
      .post(uri"http://159.75.36.118:5448/?buffer_size=1024&wait_end_of_query=1")
      .header("Connection", "Close")
      .header("Content-Type", "text/plain")
      .auth
      .basic("default", "")
      .response(asByteArray)
      .body(
        "SELECT app_id, platform, id, user_id, device_id, category, arch, os_ver, ip, region, version model FROM tencent_public.singleton_dist WHERE category = 'PERF_CRASH' and data_time >= '2021-12-20 20:00:00' LIMIT 100000 FORMAT Arrow"
      )

    val backend = HttpURLConnectionBackend()
    val response = myRequest.send(backend)
    assert(response.code == StatusCode.Ok)
    val body = response.body match {
      case Left(x) => fail(s"HTTP return error: $x")
      case Right(x) => x
    }
    val channel =
      new SeekableInMemoryByteChannel(body)

    val seekableReadChannel = new SeekableReadChannel(channel)
    val arrowFileReader =
      new ArrowFileReader(
        seekableReadChannel,
        new RootAllocator(Integer.MAX_VALUE)
      )

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
      val vectors = root.getFieldVectors
      println(vectors.size())
      val fieldVectorItr = vectors.iterator().asScala
      fieldVectorItr.foreach { vector =>
        println(vector.getMinorType)
      }
      val sparkVectors = fieldVectorItr.map[ColumnVector] { vector =>
        new ArrowColumnVector(vector)
      }.toArray
      val batch = new ColumnarBatch(sparkVectors, root.getRowCount)
      println(batch.numCols())
      println(batch.numRows())
    }

  }

  test("arrow stream from file") {
    val allocator = new RootAllocator(Integer.MAX_VALUE)
    val arrowStream = getClass.getResourceAsStream("/table_engines.arrow_stream")
    val arrowStreamReader = new ArrowStreamReader(
      arrowStream,
      allocator
    )

    val root = arrowStreamReader.getVectorSchemaRoot
    println(s"schema is ${root.getSchema}")
    println(s"bytes read: ${arrowStreamReader.bytesRead()}")

    while (arrowStreamReader.loadNextBatch()) {
      println(s"num of row in this block: ${root.getRowCount}")
      println(s"bytes read: ${arrowStreamReader.bytesRead()}")
      val vectors = root.getFieldVectors.asScala
      val fieldVectorItr = vectors.iterator
      val sparkVectors = fieldVectorItr.map[ColumnVector] { vector =>
        new ArrowColumnVector(vector)
      }.toArray
      val batch = new ColumnarBatch(sparkVectors, root.getRowCount)
      println(s"batch has cols: ${batch.numCols()}")
      println(s"batch has rows: ${batch.numRows()}")
    }

  }

  test("arrow from file") {
    val arrowFileStream = getClass.getResourceAsStream("/table_engines.arrow")
    val channel =
      new SeekableInMemoryByteChannel(IOUtils.toByteArray(arrowFileStream))

    val seekableReadChannel = new SeekableReadChannel(channel)
    val arrowFileReader =
      new ArrowFileReader(
        seekableReadChannel,
        new RootAllocator(Integer.MAX_VALUE)
      )

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
      val vectors = root.getFieldVectors
      println(vectors.size())
      val fieldVectorItr = vectors.iterator().asScala
      fieldVectorItr.foreach { vector =>
        println(vector.getMinorType)
      }
      val sparkVectors = fieldVectorItr.map[ColumnVector] { vector =>
        new ArrowColumnVector(vector)
      }.toArray
      val batch = new ColumnarBatch(sparkVectors, root.getRowCount)
      println(batch.numCols())
      println(batch.numRows())
    }

  }
}
