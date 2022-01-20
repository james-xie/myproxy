package com.gllue.myproxy.transport.backend.connection;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.transport.backend.command.CommandResultReader;
import com.gllue.myproxy.transport.core.connection.Connection;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnection;
import com.gllue.myproxy.transport.protocol.packet.command.CommandPacket;
import com.google.common.util.concurrent.ListenableFuture;

public interface BackendConnection extends Connection {
  void setDataSourceName(String dataSourceName);

  String getDataSourceName();

  long getDatabaseThreadId();

  void bindFrontendConnection(FrontendConnection frontendConnection);

  void unbindFrontendConnection();

  FrontendConnection getFrontendConnection();

  void setCommandResultReader(CommandResultReader reader);

  CommandResultReader getCommandResultReader();

  boolean readingResponse();

  void onResponseReceived();

  void setCommandExecutionDone();

  void sendCommand(CommandPacket packet, CommandResultReader reader);

  ListenableFuture<CommandResult> sendCommand(CommandPacket packet);

  void reset(Callback<CommandResult> callback);
}
