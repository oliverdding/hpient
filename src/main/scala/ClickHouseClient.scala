package com.github.oliverdding.hpient

import client.ClickHouseClientHandler

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.Unpooled
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.{ChannelInitializer, ChannelOption}
import io.netty.handler.codec.http._
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import io.netty.handler.stream.ChunkedNioStream
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.util.concurrent.Future
import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.Base64

/**
 * Client that directly handle query with clickhouse
 */
class ClickHouseClient()(implicit context: ClickHouseContext) extends AutoCloseable with Logging {

  private lazy val workerGroup = new NioEventLoopGroup()

  private lazy val b = new Bootstrap()
    .group(workerGroup)
    .channel(classOf[NioSocketChannel])
    .remoteAddress(context.host, context.port)
    .handler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel): Unit =
        ch.pipeline()
          .addLast("log", new LoggingHandler(LogLevel.INFO))
          .addLast("timeout", new ReadTimeoutHandler(60)) // TODO: best read timeout
          .addLast("codec", new HttpClientCodec())
          .addLast("decompressor", new HttpContentDecompressor())
          .addLast("aggregator", new HttpObjectAggregator(10 * 1024 * 1024)) // TODO: best buffer size
          .addLast("clickhouse", new ClickHouseClientHandler())

    })

  /**
   * Build a request on the given path and sql.
   *
   * Read username and password from ClickHouseContext of this object.
   * @param path
   *   url path
   * @param sql
   *   sql string
   * @return
   *   [[DefaultFullHttpRequest]] object
   */
  private def build(path: String, sql: String): DefaultFullHttpRequest = {
    val request =
      new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, path, Unpooled.wrappedBuffer(sql.getBytes()))
    request.headers()
      .set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.CHUNKED)
      .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE)
      .set(HttpHeaderNames.USER_AGENT, "spark-clickhouse-connector")
      .set(
        HttpHeaderNames.AUTHORIZATION,
        "Basic " + Base64.getEncoder.encodeToString(s"${context.username}:${context.password}".getBytes())
      )
    request
  }

  override def close(): Unit =
    workerGroup.shutdownGracefully()

  def asyncQuery(sql: String): Future[ColumnarBatch] = {
    val request = build("/", sql)

    val channel = b.connect().sync().channel()
    channel.pipeline().get(classOf[ClickHouseClientHandler]).query(request)
  }

}
