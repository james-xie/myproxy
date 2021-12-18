package com.gllue.myproxy.transport.frontend.connection;

import com.gllue.myproxy.transport.backend.connection.BackendConnection;
import com.gllue.myproxy.transport.core.connection.Connection;

public interface FrontendConnection extends Connection {
  String getDataSourceName();

  void bindBackendConnection(BackendConnection backendConnection);

  BackendConnection getBackendConnection();
}
