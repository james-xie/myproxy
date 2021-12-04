package com.gllue.command.handler;

public interface HandlerRequest {
  int getFrontendConnectionId();

  int getBackendConnectionId();

  String getDatasource();

  String getDatabase();
}
