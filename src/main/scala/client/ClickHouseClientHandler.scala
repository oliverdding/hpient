package com.github.oliverdding.hpient
package client

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http.{DefaultFullHttpRequest, HttpObject}
import io.netty.util.concurrent.{Future, Promise}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Because [[ClickHouseClientHandler]] contains status, every channel can only execute one query, after that should be
 * closed.
 *
 * {{{
 *   val future = channel.pipeline().get(classOf[ClickHouseClientHandler]).query(request)
 * }}}
 */
class ClickHouseClientHandler extends SimpleChannelInboundHandler[HttpObject] with Logging {

  private var ctx: Option[ChannelHandlerContext] = None
  private var promise: Option[Promise[ColumnarBatch]] = None

  override def channelRead0(ctx: ChannelHandlerContext, msg: HttpObject): Unit = {

  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    super.channelActive(ctx)
    this.ctx = Some(ctx)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    super.channelInactive(ctx)
    this.synchronized {
      if(promise.isEmpty) {
        throw new IllegalStateException("promise is empty")
      }
    }
  }

  // Query API
  def query(request: DefaultFullHttpRequest): Future[ColumnarBatch] = {
    if (ctx.isEmpty) {
      throw new IllegalStateException("should be called with active channel")
    }
    query(request, ctx.get.executor().newPromise())
  }

  private def query(request: DefaultFullHttpRequest, promise: Promise[ColumnarBatch]): Future[ColumnarBatch] =
    this.synchronized {
      this.promise = Some(promise)
      ctx.get.writeAndFlush(request)
      return promise
    }

}
