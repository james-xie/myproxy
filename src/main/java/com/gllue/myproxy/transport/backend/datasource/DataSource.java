package com.gllue.myproxy.transport.backend.datasource;

import com.gllue.myproxy.common.concurrent.ExtensibleFuture;
import com.gllue.myproxy.transport.backend.connection.ConnectionArguments;
import com.gllue.myproxy.transport.core.connection.Connection;
import javax.annotation.Nullable;

public interface DataSource<T extends Connection> {
  String getName();

  int acquiredConnections();

  int cachedConnections();

  int maxCapacity();

  void registerConnection(T connection);

  T acquireConnection(@Nullable String database);

  ExtensibleFuture<T> tryAcquireConnection(@Nullable String database);

  void releaseConnection(T connection);

  void closeConnection(T connection);

  ConnectionArguments getConnectionArguments(String database);
}
