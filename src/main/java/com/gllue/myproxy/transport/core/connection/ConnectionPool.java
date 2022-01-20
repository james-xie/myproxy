package com.gllue.myproxy.transport.core.connection;

import com.gllue.myproxy.common.concurrent.ExtensibleFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

public interface ConnectionPool extends AutoCloseable {
  int getMaxPoolSize();

  int getNumberOfAcquiredConnections();

  int getNumberOfCachedConnections();

  Connection acquireConnection(@Nullable String database);

  Connection acquireConnection(@Nullable String database, long timeout, TimeUnit unit);

  ExtensibleFuture<Connection> tryAcquireConnection(@Nullable String database);

  void releaseConnection(Connection connection);

  interface PoolEntry {
    Connection getConnection();
  }
}
