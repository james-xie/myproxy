package com.gllue.myproxy.transport.frontend.connection;

import com.gllue.myproxy.transport.backend.connection.BackendConnection;
import com.gllue.myproxy.transport.core.connection.Connection;

public interface FrontendConnection extends Connection {
  String getDataSourceName();

  boolean bindBackendConnection(BackendConnection backendConnection);

  BackendConnection getBackendConnection();

  SessionContext getSessionContext();

  void onCommandReceived();
}
