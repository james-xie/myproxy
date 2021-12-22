package com.gllue.myproxy.transport.backend.connection;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.transport.backend.datasource.DataSource;
import com.gllue.myproxy.transport.core.connection.Connection;
import com.gllue.myproxy.transport.protocol.packet.command.CommandPacket;
import com.gllue.myproxy.transport.backend.command.CommandResultReader;

public interface BackendConnection extends Connection {
  DataSource<BackendConnection> dataSource();

  void setDataSource(DataSource<BackendConnection> dataSource);

  void setCommandResultReader(CommandResultReader reader);

  CommandResultReader getCommandResultReader();

  void setCommandExecutionDone();

  void sendCommand(CommandPacket packet, CommandResultReader reader);

  void reset(Callback<CommandResult> callback);

  void assign();

  boolean isAssigned();

  boolean release();

  boolean isReleased();

  void releaseOrClose();
}
