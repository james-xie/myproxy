package com.gllue.myproxy.transport.backend.command;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.transport.core.connection.Connection;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import java.util.concurrent.Executor;

public interface CommandResultReader extends AutoCloseable {

  /**
   * Read command result from the given payload.
   *
   * @param payload mysql payload
   * @return true if all the command result has read completed, otherwise false.
   */
  boolean read(MySQLPayload payload);

  boolean isReadCompleted();

  void fireReadCompletedEvent();

  void bindConnection(Connection connection);

  CommandResultReader addCallback(Callback<CommandResult> callback);
}
