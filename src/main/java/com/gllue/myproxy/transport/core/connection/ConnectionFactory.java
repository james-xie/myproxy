package com.gllue.myproxy.transport.core.connection;

import com.gllue.myproxy.common.concurrent.ExtensibleFuture;
import com.gllue.myproxy.transport.backend.connection.ConnectionArguments;

public interface ConnectionFactory {
  ExtensibleFuture<Connection> newConnection(ConnectionArguments arguments, String database);
}
