package com.gllue.myproxy.transport.frontend.connection;

public interface FrontendConnectionManager {
  void registerFrontendConnection(FrontendConnection connection);

  FrontendConnection removeFrontendConnection(int connectionId);
}
