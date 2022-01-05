package com.gllue.myproxy.transport.frontend;

import com.gllue.myproxy.bootstrap.ServerContext;
import com.gllue.myproxy.common.Initializer;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.config.Configurations.Type;
import com.gllue.myproxy.config.TransportConfigPropertyKey;
import com.gllue.myproxy.transport.core.netty.MySQLPayloadCodecHandler;
import com.gllue.myproxy.transport.core.service.TransportService;
import com.gllue.myproxy.transport.frontend.command.CommandExecutionEngine;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnection;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnectionListener;
import com.gllue.myproxy.transport.frontend.netty.FrontendChannelInboundHandler;
import com.gllue.myproxy.transport.frontend.netty.auth.MySQLNativePasswordAuthenticationHandler;
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
  public String name() {
    return "frontend server";
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

    private final TransportService transportService;
    private final CommandExecutionEngine commandExecutionEngine;

    private FrontendConnectionListener newListener() {
      return new FrontendConnectionListener() {
        @Override
        public void onConnected(FrontendConnection connection) {
          transportService.registerFrontendConnection(connection);
        }

        @Override
        public void onClosed(FrontendConnection connection) {
          transportService.removeFrontendConnection(connection.connectionId());
        }
      };
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      var authHandler = new MySQLNativePasswordAuthenticationHandler();
      if (log.isTraceEnabled()) {
        ch.pipeline().addLast(new LoggingHandler(LogLevel.INFO));
      }

      ch.pipeline().addLast(new MySQLPayloadCodecHandler());
      ch.pipeline()
          .addLast(
              new FrontendChannelInboundHandler(
                  authHandler, commandExecutionEngine, newListener()));
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

  private CommandExecutionEngine newCommandExecutionEngine(ServerContext context) {
    return new CommandExecutionEngine(
        context.getThreadPool(),
        context.getTransportService(),
        context.getPersistRepository(),
        context.getConfigurations(),
        context.getClusterState(),
        context.getSqlParser(),
        context.getIdGenerator());
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
        new ServerChannelInitializer(
            context.getTransportService(), newCommandExecutionEngine(context));

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
