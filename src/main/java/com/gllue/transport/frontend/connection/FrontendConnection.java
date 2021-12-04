package com.gllue.transport.frontend.connection;

import com.gllue.transport.backend.connection.BackendConnection;
import com.gllue.transport.core.connection.Connection;

public interface FrontendConnection extends Connection {
  String getDataSourceName();

  void bindBackendConnection(BackendConnection backendConnection);

  BackendConnection getBackendConnection();
}
