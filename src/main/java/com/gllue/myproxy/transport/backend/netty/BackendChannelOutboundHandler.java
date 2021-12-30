package com.gllue.myproxy.transport.backend.netty;

import com.gllue.myproxy.common.concurrent.AbstractRunnable;
import com.gllue.myproxy.common.util.ReflectionUtils;
import com.gllue.myproxy.transport.backend.BackendHandshakeException;
import com.gllue.myproxy.transport.backend.connection.BackendConnection;
import com.gllue.myproxy.transport.backend.connection.BackendConnectionImpl;
import com.gllue.myproxy.transport.backend.connection.BackendConnectionListener;
import com.gllue.myproxy.transport.backend.connection.ConnectionArguments;
import com.gllue.myproxy.transport.backend.netty.auth.AuthenticationPluginHandler;
import com.gllue.myproxy.transport.backend.netty.auth.AuthenticationState;
import com.gllue.myproxy.transport.backend.netty.auth.CachingSha2PluginHandler;
import com.gllue.myproxy.transport.backend.netty.auth.NativePasswordPluginHandler;
import com.gllue.myproxy.transport.constant.MySQLAuthenticationMethod;
import com.gllue.myproxy.transport.constant.MySQLCapabilityFlag;
import com.gllue.myproxy.transport.constant.MySQLServerInfo;
import com.gllue.myproxy.transport.constant.MySQLStatusFlag;
import com.gllue.myproxy.transport.core.connection.AuthenticationData;
import com.gllue.myproxy.transport.core.connection.ConnectionIdGenerator;
import com.gllue.myproxy.transport.core.netty.NettyUtils;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import com.gllue.myproxy.transport.protocol.packet.handshake.AuthMoreDataPacket;
import com.gllue.myproxy.transport.protocol.packet.handshake.AuthPacketWrapper;
import com.gllue.myproxy.transport.protocol.packet.handshake.AuthSwitchRequestPacket;
import com.gllue.myproxy.transport.protocol.packet.handshake.HandshakeResponsePacket41;
import com.gllue.myproxy.transport.protocol.packet.handshake.InitialHandshakePacketV10;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackendChannelOutboundHandler extends ChannelInboundHandlerAdapter {

  private static final Map<String, Class<? extends AuthenticationPluginHandler>>
      authPluginHandlers = new HashMap<>();

  static {
    authPluginHandlers.put("", NativePasswordPluginHandler.class);
    authPluginHandlers.put(
        MySQLAuthenticationMethod.NATIVE_PASSWORD.getMethodName(),
        NativePasswordPluginHandler.class);
    authPluginHandlers.put(
        MySQLAuthenticationMethod.CACHING_SHA2.getMethodName(), CachingSha2PluginHandler.class);
  }

  private enum ConnectionPhase {
    HANDSHAKE,
    CHECK_RESPONSE,
    AUTHENTICATION,
    CONNECTED,
    FAILED
  }

  private final ConnectionArguments connectionArguments;

  private final BackendConnectionListener backendConnectionListener;

  private final AuthenticationData authData;

  private int statusFlags;

  private int connectionId;

  private ConnectionPhase connectionPhase = ConnectionPhase.HANDSHAKE;

  private AuthenticationState authState;

  private AuthenticationPluginHandler authPluginHandler;

  private BackendConnection connection;

  public BackendChannelOutboundHandler(
      final ConnectionArguments connectionArguments,
      final BackendConnectionListener backendConnectionListener) {
    this.connectionArguments = connectionArguments;
    this.backendConnectionListener = backendConnectionListener;
    this.authData =
        new AuthenticationData(
            connectionArguments.getUsername(),
            connectionArguments.getPassword(),
            connectionArguments.getDatabase());
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    log.info("Connected to backend database [{}]", ctx.channel().remoteAddress());
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    log.info("Disconnect from backend database [{}]", ctx.channel().remoteAddress());
    if (connection.getCommandResultReader() != null) {
      try {
        connection.setCommandExecutionDone();
      } catch (Exception e) {
        log.error("An error was occurred when invoking connection.setCommandExecutionDone().", e);
      }
    }
    backendConnectionListener.onClosed(connection);
    connection = null;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
    if (!(message instanceof MySQLPayload)) {
      log.error(
          "Got a unknown message type on channel read. [{}]", message.getClass().getSimpleName());
      return;
    }

    try (var payload = (MySQLPayload) message) {
      switch (connectionPhase) {
        case HANDSHAKE:
          handshake(ctx, payload);
          break;
        case CHECK_RESPONSE:
          checkResponse(ctx, payload);
          break;
        case AUTHENTICATION:
          authentication(ctx, payload, null);
          break;
        case CONNECTED:
          receiveResponse(ctx, payload);
          break;
        case FAILED:
          log.error("Illegal state exception, got an unexpected [FAILED] phase.");
          NettyUtils.closeChannel(ctx.channel(), false);
          break;
      }
    }

    if (log.isDebugEnabled()) {
      if (connectionPhase != ConnectionPhase.CONNECTED) {
        log.debug("Connection phase. [{}]", connectionPhase);
      }
    }
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
    if (connection != null) {
      connection.onWritabilityChanged();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (log.isErrorEnabled()) {
      log.debug("Caught a connection exception.", cause);
    }
    super.exceptionCaught(ctx, cause);
  }

  private void handshake(final ChannelHandlerContext ctx, final MySQLPayload payload) {
    var initialHandshakePacket = new InitialHandshakePacketV10(payload);

    var pluginName = initialHandshakePacket.getAuthPluginName();

    authPluginHandler = getAuthPluginHandlerByName(pluginName);
    if (authPluginHandler == null) {
      var errMsg = String.format("Unsupported auth plugin name [%s].", pluginName);
      onFailure(ctx, new BackendHandshakeException(errMsg));
      return;
    }

    if (log.isDebugEnabled()) {
      log.debug("Auth plugin name: " + pluginName);
    }

    int serverCapabilityFlags = initialHandshakePacket.getCapabilityFlags();
    if (!MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH.isBitSet(serverCapabilityFlags)) {
      onFailure(
          ctx,
          new BackendHandshakeException(
              "Cannot connect to the server which has no [CLIENT_PLUGIN_AUTH] capability."));
      return;
    }

    statusFlags = initialHandshakePacket.getStatusFlags();
    connectionId = initialHandshakePacket.getConnectionId();
    authData.setAuthResponse(initialHandshakePacket.getAuthPluginData());

    byte[] authResponse;
    try {
      authResponse =
          authPluginHandler.scramblePassword(
              connectionArguments.getPassword(), authData.getAuthResponse());
    } catch (Exception e) {
      onFailure(ctx, e);
      return;
    }

    var clientCapabilityFlags =
        MySQLCapabilityFlag.handshakeClientCapabilityFlags(serverCapabilityFlags);
    if (connectionArguments.getDatabase() == null) {
      clientCapabilityFlags &= ~MySQLCapabilityFlag.CLIENT_CONNECT_WITH_DB.getValue();
    }

    var handshakeResponsePacket =
        new HandshakeResponsePacket41(
            clientCapabilityFlags,
            MySQLPacket.MAX_PACKET_SIZE,
            MySQLServerInfo.DEFAULT_CHARSET,
            connectionArguments.getUsername(),
            authResponse,
            connectionArguments.getDatabase(),
            pluginName);
    ctx.writeAndFlush(handshakeResponsePacket);
    authState = authPluginHandler.getInitialState();
    connectionPhase = ConnectionPhase.CHECK_RESPONSE;
  }

  private void checkResponse(final ChannelHandlerContext ctx, final MySQLPayload payload) {
    var wrapper = AuthPacketWrapper.newInstance(payload);
    BackendHandshakeException exception = null;
    if (wrapper.isOkPacket()) {
      var registered = onConnected(ctx);
      if (!registered) {
        exception = new BackendHandshakeException("Registration failed.");

        if (log.isDebugEnabled()) {
          log.debug("Failed to register backend connection.");
        }
      }
    } else if (wrapper.isAuthSwitchPacket()) {
      if (log.isDebugEnabled()) {
        log.debug("Received auth switch request.");
      }

      handleAuthSwitch(ctx, (AuthSwitchRequestPacket) wrapper.getPacket());
    } else if (wrapper.isAuthMoreDataPacket()) {
      if (log.isDebugEnabled()) {
        log.debug("Received auth extra data.");
      }

      handleExtraAuthData(ctx, (AuthMoreDataPacket) wrapper.getPacket());
    } else if (wrapper.isErrPacket()) {
      var packet = (ErrPacket) wrapper.getPacket();
      exception = new BackendHandshakeException(packet.getErrorMessage());

      if (log.isDebugEnabled()) {
        log.debug(
            "Failed to connect to backend database due to an error was occurred. [errorCode:{}]",
            packet.getErrorCode());
      }
    } else {
      exception = new BackendHandshakeException("Unrecognized packet");

      if (log.isDebugEnabled()) {
        log.debug(
            "Failed to connect to backend database due to got an unrecognized packet. [payloadHex:{}]",
            payload);
      }
    }

    if (exception != null) {
      onFailure(ctx, exception);
    }
  }

  private AuthenticationPluginHandler getAuthPluginHandlerByName(final String pluginName) {
    var handlerClass = authPluginHandlers.get(pluginName);
    if (handlerClass == null) {
      return null;
    }

    AuthenticationPluginHandler handler = null;
    try {
      handler = ReflectionUtils.newInstanceWithNoArgsConstructor(handlerClass);
    } catch (Exception e) {
      log.error("Failed to instantiate authentication plugin handler.", e);
    }
    return handler;
  }

  private void handleAuthSwitch(
      final ChannelHandlerContext ctx, final AuthSwitchRequestPacket packet) {
    authPluginHandler = getAuthPluginHandlerByName(packet.getPluginName());
    if (authPluginHandler == null) {
      var errMsg = String.format("Unsupported auth plugin name [%s].", packet.getPluginName());
      onFailure(ctx, new BackendHandshakeException(errMsg));
      return;
    }

    if (log.isDebugEnabled()) {
      log.debug("Switching auth plugin to " + packet.getPluginName());
    }

    authState = authPluginHandler.getInitialState();
    connectionPhase = ConnectionPhase.AUTHENTICATION;
    authentication(ctx, null, packet);
  }

  private void handleExtraAuthData(
      final ChannelHandlerContext ctx, final AuthMoreDataPacket packet) {
    connectionPhase = ConnectionPhase.AUTHENTICATION;
    authentication(ctx, null, packet);
  }

  private void authentication(
      final ChannelHandlerContext ctx, final MySQLPayload payload, final MySQLPacket packet) {
    assert authPluginHandler != null;

    try {
      authState = authPluginHandler.authenticate(ctx, payload, packet, authState, authData);
    } catch (Exception e) {
      onFailure(ctx, new BackendHandshakeException(e));
      return;
    }

    if (log.isDebugEnabled()) {
      log.debug("Authentication state: " + authState);
    }

    if (authState == authState.failedState()) {
      onFailure(ctx, new BackendHandshakeException("Authentication failure."));
    } else if (authState == authState.successState()) {
      var registered = onConnected(ctx);
      if (!registered) {
        onFailure(ctx, new BackendHandshakeException("Registration failed."));
      }
    }
  }

  private void onFailure(final ChannelHandlerContext ctx, final Exception e) {
    log.error("Backend connection has broken. [{}]", e.getMessage());
    connectionPhase = ConnectionPhase.FAILED;
    backendConnectionListener.onConnectFailed(e);
    NettyUtils.closeChannel(ctx.channel(), false);
  }

  private boolean onConnected(final ChannelHandlerContext ctx) {
    log.info("Backend connection has connected.");
    assert connection == null;
    connectionPhase = ConnectionPhase.CONNECTED;
    connection = new BackendConnectionImpl(connectionId, ctx.channel());
    if (MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT.isBitSet(statusFlags)) {
      connection.enableAutoCommit();
    } else {
      connection.disableAutoCommit();
    }
    connection.changeDatabase(authData.getDatabaseName());
    return backendConnectionListener.onConnected(connection);
  }

  private void receiveResponse(final ChannelHandlerContext ctx, final MySQLPayload payload) {
    var commandResultReader = connection.getCommandResultReader();
    if (commandResultReader == null) {
      throw new IllegalStateException("Command result reader is empty during receiving response.");
    }

    commandResultReader
        .executor()
        .execute(
            new AbstractRunnable() {
              @Override
              protected void doRun() throws Exception {
                var readCompleted = commandResultReader.read(payload);
                if (readCompleted) {
                  connection.setCommandExecutionDone();
                }
              }

              @Override
              public void onFailure(Exception e) {
                log.error("An error was occurred when reading the command result.", e);
                NettyUtils.closeChannel(ctx.channel(), false);
              }
            });
  }
}
