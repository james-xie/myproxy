package com.gllue.myproxy.transport.core.connection;

import java.util.Collection;

public interface IdleConnectionDetector {
  void register(Connection connection);

  void remove(Connection connection);

  Collection<Connection> detectIdleConnections(long currentTime);
}
