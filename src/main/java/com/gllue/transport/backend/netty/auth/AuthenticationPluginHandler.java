package com.gllue.transport.backend.netty.auth;

import com.gllue.transport.core.connection.AuthenticationData;
import com.gllue.transport.protocol.packet.MySQLPacket;
import com.gllue.transport.protocol.payload.MySQLPayload;
import io.netty.channel.ChannelHandlerContext;

public interface AuthenticationPluginHandler {
  byte[] scramblePassword(String password, byte[] salt) throws Exception;

  AuthenticationState authenticate(
      ChannelHandlerContext ctx,
      MySQLPayload payload,
      MySQLPacket packet,
      AuthenticationState currentState,
      AuthenticationData authData)
      throws Exception;

  AuthenticationState getInitialState();
}
