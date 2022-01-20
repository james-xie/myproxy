package com.gllue.myproxy.transport.backend.datasource;

import com.gllue.myproxy.common.concurrent.ExtensibleFuture;
import com.gllue.myproxy.transport.backend.connection.ConnectionArguments;
import com.gllue.myproxy.transport.core.connection.Connection;
import javax.annotation.Nullable;

public interface DataSource {
  String getName();

  Connection getConnection(@Nullable String database);

  ExtensibleFuture<Connection> tryGetConnection(@Nullable String database);

  ConnectionArguments getConnectionArguments(String database);
}
