package com.gllue.myproxy.command.handler;

public interface HandlerRequest {
  int getConnectionId();

  String getDatasource();

  String getDatabase();
}
