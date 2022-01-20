package com.gllue.myproxy.transport.frontend.netty;

import com.gllue.myproxy.constant.ServerConstants;
import com.gllue.myproxy.transport.backend.datasource.DataSource;
import com.gllue.myproxy.transport.constant.MySQLAuthenticationMethod;
import com.gllue.myproxy.transport.constant.MySQLCapabilityFlag;
import com.gllue.myproxy.transport.constant.MySQLCharsets;
import com.gllue.myproxy.transport.constant.MySQLCommandPacketType;
import com.gllue.myproxy.transport.constant.MySQLServerInfo;
import com.gllue.myproxy.transport.constant.MySQLStatusFlag;
import com.gllue.myproxy.transport.core.connection.AuthenticationData;
import com.gllue.myproxy.transport.core.connection.ConnectionIdGenerator;
import com.gllue.myproxy.transport.core.netty.NettyUtils;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
import com.gllue.myproxy.transport.exception.ServerErrorCode;
import com.gllue.myproxy.transport.exception.UnsupportedCommandException;
import com.gllue.myproxy.transport.frontend.command.CommandExecutionEngine;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnection;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnectionImpl;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnectionListener;
import com.gllue.myproxy.transport.frontend.netty.auth.AuthenticationHandler;
import com.gllue.myproxy.transport.protocol.packet.command.CommandPacket;
import com.gllue.myproxy.transport.protocol.packet.command.CreateDBCommandPacket;
import com.gllue.myproxy.transport.protocol.packet.command.DropDBCommandPacket;
import com.gllue.myproxy.transport.protocol.packet.command.FieldListCommandPacket;
import com.gllue.myproxy.transport.protocol.packet.command.InitDBCommandPacket;
import com.gllue.myproxy.transport.protocol.packet.command.ProcessKillCommandPacket;
import com.gllue.myproxy.transport.protocol.packet.command.QueryCommandPacket;
import com.gllue.myproxy.transport.protocol.packet.command.SimpleCommandPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.OKPacket;
import com.gllue.myproxy.transport.protocol.packet.handshake.AuthSwitchRequestPacket;
import com.gllue.myproxy.transport.protocol.packet.handshake.AuthSwitchResponsePacket;
import com.gllue.myproxy.transport.protocol.packet.handshake.HandshakeResponsePacket41;
import com.gllue.myproxy.transport.protocol.packet.handshake.InitialHandshakePacketV10;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import com.google.common.base.Strings;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class FrontendChannelInboundHandler extends ChannelInboundHandlerAdapter {

  private static final ConnectionIdGenerator ID_GENERATOR = new ConnectionIdGenerator(1);

  private enum ConnectionPhase {
    INITIAL_HANDSHAKE,
    FAST_PATH,
    AUTHENTICATION_METHOD_SWITCH,
    AUTHENTICATED,
    FAILED
  }

  private ConnectionPhase connectionPhase = ConnectionPhase.INITIAL_HANDSHAKE;

  private final AuthenticationHandler authHandler;

  private final CommandExecutionEngine commandExecuteEngine;

  private final FrontendConnectionListener frontendConnectionListener;

  private AuthenticationData authData;

  private Charset clientCharset = null;

  private int connectionId;

  private FrontendConnection frontendConnection;

  public FrontendChannelInboundHandler(
      final AuthenticationHandler authHandler,
      final CommandExecutionEngine commandExecutionEngine,
      final FrontendConnectionListener connectionManager) {
    this.authHandler = authHandler;
    this.commandExecuteEngine = commandExecutionEngine;
    this.frontendConnectionListener = connectionManager;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    log.info("Connection has accepted. [{}]", ctx.channel().remoteAddress());

    initialHandshake(ctx);
    connectionPhase = ConnectionPhase.FAST_PATH;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
    if (!(message instanceof MySQLPayload)) {
      log.error(
          "Got a unknown message type on channel read. [{}]", message.getClass().getSimpleName());
      return;
    }

    // Release the ByteBuf in the message.
    try (var payload = (MySQLPayload) message) {
      if (clientCharset != null) {
        payload.setCharset(clientCharset);
      }

      switch (connectionPhase) {
        case FAST_PATH:
          authFastPath(ctx, payload);
          break;
        case AUTHENTICATION_METHOD_SWITCH:
          authMethodSwitch(ctx, payload);
          break;
        case AUTHENTICATED:
          processCommand(payload);
          break;
        case FAILED:
          sendErrorPacket(ctx, ServerErrorCode.ER_SERVER_ERROR, "illegalConnectionState: FAILED");
          NettyUtils.closeChannel(ctx.channel(), false);
          break;
      }
    }

    if (log.isDebugEnabled()) {
      if (connectionPhase != ConnectionPhase.AUTHENTICATED) {
        log.info("Connection phase. [{}]", connectionPhase);
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    connectionPhase = ConnectionPhase.FAILED;
    log.error("Caught a connection exception.", cause);
    var message = cause.getClass().getName() + ": " + cause.getMessage();
    ctx.writeAndFlush(new ErrPacket(ServerErrorCode.ER_SERVER_ERROR, message))
        .addListener((future) -> NettyUtils.closeChannel(ctx.channel(), false));
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
    if (frontendConnection != null) {
      frontendConnection.onWritabilityChanged();
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (frontendConnection != null) {
      frontendConnection.close();
      frontendConnectionListener.onClosed(frontendConnection);
      frontendConnection = null;
    }

    log.info("Connection has closed. [{}]", ctx.channel().remoteAddress());
  }

  private void initialHandshake(final ChannelHandlerContext ctx) {
    connectionId = ID_GENERATOR.nextId();
    var packet =
        new InitialHandshakePacketV10(
            ServerConstants.getServerVersion(),
            connectionId,
            authHandler.getAuthPluginData(),
            MySQLCapabilityFlag.handshakeServerCapabilityFlags(),
            MySQLServerInfo.DEFAULT_CHARSET,
            MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT.getValue(),
            MySQLAuthenticationMethod.NATIVE_PASSWORD.getMethodName());
    ctx.channel().writeAndFlush(packet);
  }

  private String[] parseUserName(String userName) {
    if (Strings.isNullOrEmpty(userName)) {
      return null;
    }

    var index = userName.indexOf('#');
    if (index < 0) {
      // Use the default data source.
      return new String[] {DataSource.DEFAULT, userName};
    }
    if (index > 0 && index < userName.length() - 1) {
      // The userName format: "datasource#username"
      return userName.split("#", 2);
    }
    return null;
  }

  private void authFastPath(final ChannelHandlerContext ctx, final MySQLPayload payload) {
    var handshakeResponse = new HandshakeResponsePacket41(payload);
    var userName = handshakeResponse.getUsername();
    var userNameTuple = parseUserName(userName);
    if (userNameTuple == null) {
      connectionPhase = ConnectionPhase.FAILED;
      sendErrorPacket(ctx, MySQLServerErrorCode.ER_NO_SUCH_USER, userName);
      return;
    }

    var dataSource = userNameTuple[0];
    var realUserName = userNameTuple[1];
    var database = handshakeResponse.getDatabase();
    if (!checkDataSourceExists(dataSource) || !checkDatabaseExists(database)) {
      connectionPhase = ConnectionPhase.FAILED;
      sendErrorPacket(ctx, MySQLServerErrorCode.ER_BAD_DB_ERROR);
      return;
    }

    authData =
        new AuthenticationData(
            realUserName,
            getHostAddress(ctx),
            null,
            handshakeResponse.getDatabase(),
            handshakeResponse.getAuthResponse(),
            dataSource);

    clientCharset = MySQLCharsets.getCharsetById(handshakeResponse.getCharset()).charset();

    if (handshakeResponse.isClientPluginAuth() && !isNativePasswordAuth(handshakeResponse)) {
      connectionPhase = ConnectionPhase.AUTHENTICATION_METHOD_SWITCH;
      sendAuthSwitchRequestPacket(ctx);
      return;
    }

    authenticate(ctx);
  }

  private void sendAuthSwitchRequestPacket(ChannelHandlerContext ctx) {
    var authSwitchRequest =
        new AuthSwitchRequestPacket(
            MySQLAuthenticationMethod.NATIVE_PASSWORD.getMethodName(),
            new String(authHandler.getAuthPluginData()));
    ctx.writeAndFlush(authSwitchRequest);
  }

  private void sendOkPacket(final ChannelHandlerContext ctx) {
    ctx.writeAndFlush(new OKPacket());
  }

  private void sendErrorPacket(
      final ChannelHandlerContext ctx, final SQLErrorCode errorCode, final Object... args) {
    ErrPacket packet;
    String username = "";
    String hostname = "";
    String database = "";
    if (authData != null) {
      username = authData.getUsername();
      hostname = authData.getHostname();
      database = authData.getDatabaseName();
    }

    if (MySQLServerErrorCode.ER_ACCESS_DENIED_ERROR.equals(errorCode)) {
      packet = new ErrPacket(errorCode, username, hostname, usingPasswordMessage());
    } else if (MySQLServerErrorCode.ER_BAD_DB_ERROR.equals(errorCode)) {
      packet = new ErrPacket(errorCode, database);
    } else {
      packet = new ErrPacket(errorCode, args);
    }
    ctx.writeAndFlush(packet);
  }

  private boolean checkUserNameFormat(final String userName) {
    if (Strings.isNullOrEmpty(userName)) {
      return false;
    }

    // User name format: "datasource#username"
    var index = userName.indexOf('#');
    return index > 0 && index < userName.length() - 1;
  }

  // todo: implement it
  private boolean checkDataSourceExists(final String dataSource) {
    if (Strings.isNullOrEmpty(dataSource)) {
      return false;
    }
    return true;
  }

  // todo: implement it
  private boolean checkDatabaseExists(final String database) {
    if (Strings.isNullOrEmpty(database)) {
      return true;
    }
    return true;
  }

  private boolean isNativePasswordAuth(HandshakeResponsePacket41 packet) {
    return MySQLAuthenticationMethod.NATIVE_PASSWORD
        .getMethodName()
        .equals(packet.getAuthPluginName());
  }

  private String getHostAddress(final ChannelHandlerContext context) {
    SocketAddress socketAddress = context.channel().remoteAddress();
    return socketAddress instanceof InetSocketAddress
        ? ((InetSocketAddress) socketAddress).getAddress().getHostAddress()
        : socketAddress.toString();
  }

  private String usingPasswordMessage() {
    return authData.getAuthResponse().length == 0 ? "NO" : "YES";
  }

  private void authMethodSwitch(final ChannelHandlerContext ctx, final MySQLPayload payload) {
    var authSwitchResponse = new AuthSwitchResponsePacket(payload);
    authData.setAuthResponse(authSwitchResponse.getAuthPluginData());

    authenticate(ctx);
  }

  private void authenticate(final ChannelHandlerContext ctx) {
    if (authHandler.authenticate(authData)) {
      if (log.isDebugEnabled()) {
        log.debug("Connection-{} authenticated.", connectionId);
      }
      connectionPhase = ConnectionPhase.AUTHENTICATED;
      sendOkPacket(ctx);
      frontendConnection =
          new FrontendConnectionImpl(
              connectionId, authData.getUsername(), ctx.channel(), authData.getDataSource());
      frontendConnectionListener.onConnected(frontendConnection);

      var database = authData.getDatabaseName();
      if (!Strings.isNullOrEmpty(database)) {
        frontendConnection.changeDatabase(database);
      }
    } else {
      connectionPhase = ConnectionPhase.FAILED;
      sendErrorPacket(ctx, MySQLServerErrorCode.ER_ACCESS_DENIED_ERROR);
    }
  }

  private void processCommand(final MySQLPayload payload) {
    final var commandType = MySQLCommandPacketType.valueOf(payload.peek());
    final CommandPacket packet;
    switch (commandType) {
      case COM_QUIT:
      case COM_STATISTICS:
      case COM_PROCESS_INFO:
      case COM_PING:
        packet = new SimpleCommandPacket(commandType);
        break;
      case COM_INIT_DB:
        packet = new InitDBCommandPacket(payload);
        break;
      case COM_QUERY:
        packet = new QueryCommandPacket(payload);
        break;
      case COM_FIELD_LIST:
        packet = new FieldListCommandPacket(payload);
        break;
      case COM_CREATE_DB:
        packet = new CreateDBCommandPacket(payload);
        break;
      case COM_DROP_DB:
        packet = new DropDBCommandPacket(payload);
        break;
      case COM_PROCESS_KILL:
        packet = new ProcessKillCommandPacket(payload);
        break;
      default:
        throw new UnsupportedCommandException(commandType.name());
    }

    frontendConnection.onCommandReceived();
    commandExecuteEngine.execute(frontendConnection, packet);
  }
}
