package com.gllue.transport.backend.connection;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class ConnectionArguments {
  public static final SocketAddress DEFAULT_ADDRESS =
      new InetSocketAddress(InetAddress.getLoopbackAddress(), 3306);

  private final SocketAddress socketAddress;

  private final String username;

  private final String password;

  private final String database;
}
