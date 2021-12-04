package com.gllue.transport.frontend.connection;

public interface FrontendConnectionManager {
  void registerFrontendConnection(FrontendConnection connection);

  FrontendConnection removeFrontendConnection(int connectionId);
}
