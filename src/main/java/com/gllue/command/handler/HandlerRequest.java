package com.gllue.command.handler;

public interface HandlerRequest {
  int getConnectionId();

  String getDatasource();

  String getDatabase();
}
