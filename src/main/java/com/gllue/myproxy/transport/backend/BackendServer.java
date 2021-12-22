package com.gllue.myproxy.transport.backend;

import com.gllue.myproxy.bootstrap.ServerContext;
import com.gllue.myproxy.common.Initializer;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.config.Configurations.Type;
import com.gllue.myproxy.config.TransportConfigPropertyKey;
import com.gllue.myproxy.transport.backend.connection.BackendConnectionListener;
import com.gllue.myproxy.transport.backend.connection.ConnectionArguments;
import com.gllue.myproxy.transport.backend.netty.BackendChannelOutboundHandler;
import com.gllue.myproxy.transport.core.netty.MySQLPayloadCodecHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackendServer implements Initializer {
  private static final BackendServer INSTANCE = new BackendServer();

  public static BackendServer getInstance() {
    return INSTANCE;
  }

  private Bootstrap bootstrap;

  private EventLoopGroup eventLoopGroup;

  private Configurations configurations;

  @Override
  public String name() {
    return "backend server";
  }

  @Override
  public void initialize(final ServerContext context) {
    configurations = context.getConfigurations();
    eventLoopGroup = createEventLoopGroup();
    var bootstrap = new Bootstrap();
    bootstrap.group(eventLoopGroup).channel(channelClass());
    configBootstrap(bootstrap);
    this.bootstrap = bootstrap;
  }

  public ChannelFuture connect(
      final ConnectionArguments connArgs, final BackendConnectionListener listener) {
    var bootstrap = this.bootstrap.clone(this.bootstrap.config().group());
    bootstrap.handler(new ConnectionChannelInitializer(connArgs, listener));
    return bootstrap.connect(connArgs.getSocketAddress());
  }

  @Override
  public void close() throws Exception {
    if (eventLoopGroup != null) {
      eventLoopGroup.shutdownGracefully();
    }
  }

  @RequiredArgsConstructor
  static class ConnectionChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final ConnectionArguments connArguments;
    private final BackendConnectionListener backendConnListener;

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      if (log.isTraceEnabled()) {
        ch.pipeline().addLast(new LoggingHandler(LogLevel.INFO));
      }
      ch.pipeline().addLast(new MySQLPayloadCodecHandler());
      ch.pipeline().addLast(new BackendChannelOutboundHandler(connArguments, backendConnListener));
    }
  }

  private EventLoopGroup createEventLoopGroup() {
    int workerCount =
        configurations.getValue(Type.TRANSPORT, TransportConfigPropertyKey.BACKEND_WORKER_COUNT);
    return Epoll.isAvailable()
        ? new EpollEventLoopGroup(workerCount)
        : new NioEventLoopGroup(workerCount);
  }

  private Class<? extends Channel> channelClass() {
    return Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class;
  }

  private void configBootstrap(final Bootstrap bootstrap) {
    int connect_timeout =
        configurations.getValue(
            Type.TRANSPORT, TransportConfigPropertyKey.BACKEND_CONNECT_TIMEOUT_MILLIS);
    int write_buffer_low_water_mark =
        configurations.getValue(
            Type.TRANSPORT, TransportConfigPropertyKey.BACKEND_WRITE_BUFFER_LOW_WATER_MARK);
    int write_buffer_high_water_mark =
        configurations.getValue(
            Type.TRANSPORT, TransportConfigPropertyKey.BACKEND_WRITE_BUFFER_HIGH_WATER_MARK);

    bootstrap
        .option(
            ChannelOption.WRITE_BUFFER_WATER_MARK,
            new WriteBufferWaterMark(write_buffer_low_water_mark, write_buffer_high_water_mark))
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connect_timeout);
  }
}
