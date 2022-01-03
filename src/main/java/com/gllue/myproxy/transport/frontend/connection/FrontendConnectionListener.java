package com.gllue.myproxy.transport.frontend.connection;

public interface FrontendConnectionListener {
  void onConnected(FrontendConnection connection);

  void onClosed(FrontendConnection connection);
}
