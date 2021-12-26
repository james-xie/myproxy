package com.gllue.myproxy.transport.frontend.command;

import com.gllue.myproxy.transport.core.connection.Connection;

public interface CommandResultWriter {
  void write(Connection connection);
}
