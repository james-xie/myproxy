package com.gllue.myproxy.transport.backend.datasource;

import com.gllue.myproxy.common.concurrent.ExtensibleFuture;
import com.gllue.myproxy.transport.backend.BackendConnectionException;
import com.gllue.myproxy.transport.backend.connection.BackendConnectionFactory;
import com.gllue.myproxy.transport.backend.connection.ConnectionArguments;
import com.gllue.myproxy.transport.core.connection.Connection;
import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackendDataSource implements DataSource {
  private final String name;

  private final SocketAddress socketAddress;

  private final String username;

  private final String password;

  private final BackendConnectionFactory connectionFactory;

  public BackendDataSource(
      final String name,
      final SocketAddress socketAddress,
      final String username,
      final String password,
      final BackendConnectionFactory connectionFactory) {
    this.name = name;
    this.socketAddress = socketAddress;
    this.username = username;
    this.password = password;
    this.connectionFactory = connectionFactory;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Connection getConnection(@Nullable String database) {
    var future = tryGetConnection(database);
    try {
      return future.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new BackendConnectionException(e, "Failed to create new connection.");
    }
  }

  @Override
  public ExtensibleFuture<Connection> tryGetConnection(@Nullable final String database) {
    return connectionFactory.newConnection(this, database);
  }

  @Override
  public ConnectionArguments getConnectionArguments(String database) {
    return new ConnectionArguments(socketAddress, username, password, database);
  }
}
