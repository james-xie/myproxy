package com.gllue.transport.frontend;

import com.gllue.bootstrap.ServerContext;
import com.gllue.common.Initializer;
import com.gllue.common.concurrent.ThreadPool;
import com.gllue.config.Configurations;
import com.gllue.config.Configurations.Type;
import com.gllue.config.TransportConfigPropertyKey;
import com.gllue.transport.core.service.TransportService;
import com.gllue.transport.core.netty.MySQLPayloadCodecHandler;
import com.gllue.transport.frontend.command.CommandExecutionEngine;
import com.gllue.transport.frontend.netty.auth.MySQLNativePasswordAuthenticationHandler;
import com.gllue.transport.frontend.netty.FrontendChannelInboundHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class FrontendServer implements Initializer {
  private static final FrontendServer INSTANCE = new FrontendServer();

  private Configurations configurations;

  private ServerBootstrap bootstrap;

  private EventLoopGroup eventLoopGroup;

  public static FrontendServer getInstance() {
    return INSTANCE;
  }

  @Override
  public void initialize(final ServerContext context) {
    configurations = context.getConfigurations();
    eventLoopGroup = createEventLoopGroup();
    bootstrap = new ServerBootstrap();
    bootstrap.group(eventLoopGroup).channel(channelClass());
    configServerBootstrap(bootstrap, context);
  }

  public void start() throws Exception {
    String serverAddr =
        configurations.getValue(Type.TRANSPORT, TransportConfigPropertyKey.FRONTEND_SERVER_ADDRESS);
    int serverPort =
        configurations.getValue(Type.TRANSPORT, TransportConfigPropertyKey.FRONTEND_SERVER_PORT);
    var socketAddress = new InetSocketAddress(InetAddress.getByName(serverAddr), serverPort);
    log.info("Bind address [{}]", socketAddress);
    var future = bootstrap.bind(socketAddress).sync();
    log.info("Server start success");
    future.channel().closeFuture().sync();
  }

  @Override
  public void close() throws Exception {
    if (eventLoopGroup != null) {
      eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
    }
  }

  @RequiredArgsConstructor
  static class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final ThreadPool threadPool;
    private final TransportService transportService;

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      var authHandler = new MySQLNativePasswordAuthenticationHandler();
      var engine = new CommandExecutionEngine(threadPool, transportService);
      if (log.isTraceEnabled()) {
        ch.pipeline().addLast(new LoggingHandler(LogLevel.INFO));
      }

      ch.pipeline().addLast(new MySQLPayloadCodecHandler());
      ch.pipeline()
          .addLast(new FrontendChannelInboundHandler(authHandler, engine, transportService));
    }
  }

  private EventLoopGroup createEventLoopGroup() {
    int workerCount =
        configurations.getValue(Type.TRANSPORT, TransportConfigPropertyKey.FRONTEND_WORKER_COUNT);
    return Epoll.isAvailable()
        ? new EpollEventLoopGroup(workerCount)
        : new NioEventLoopGroup(workerCount);
  }

  private Class<? extends ServerChannel> channelClass() {
    return Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
  }

  private void configServerBootstrap(final ServerBootstrap bootstrap, final ServerContext context) {
    int backlog =
        configurations.getValue(Type.TRANSPORT, TransportConfigPropertyKey.FRONTEND_BACKLOG);
    int write_buffer_low_water_mark =
        configurations.getValue(
            Type.TRANSPORT, TransportConfigPropertyKey.FRONTEND_WRITE_BUFFER_LOW_WATER_MARK);
    int write_buffer_high_water_mark =
        configurations.getValue(
            Type.TRANSPORT, TransportConfigPropertyKey.FRONTEND_WRITE_BUFFER_HIGH_WATER_MARK);
    var writeBufferWaterMark =
        new WriteBufferWaterMark(write_buffer_low_water_mark, write_buffer_high_water_mark);
    var childHandler =
        new ServerChannelInitializer(context.getThreadPool(), context.getTransportService());

    bootstrap
        .option(ChannelOption.SO_BACKLOG, backlog)
        .option(ChannelOption.WRITE_BUFFER_WATER_MARK, writeBufferWaterMark)
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.SO_REUSEADDR, true)
        .childHandler(childHandler);
  }
}
