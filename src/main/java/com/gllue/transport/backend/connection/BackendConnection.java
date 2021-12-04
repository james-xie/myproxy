package com.gllue.transport.backend.connection;

import com.gllue.command.result.CommandResult;
import com.gllue.common.Callback;
import com.gllue.transport.backend.command.CommandResultReader;
import com.gllue.transport.backend.command.QueryResultReader;
import com.gllue.transport.backend.datasource.DataSource;
import com.gllue.transport.constant.MySQLCommandPacketType;
import com.gllue.transport.core.connection.Connection;
import com.gllue.transport.protocol.packet.command.CommandPacket;

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
