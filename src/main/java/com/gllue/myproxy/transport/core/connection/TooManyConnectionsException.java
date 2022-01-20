package com.gllue.myproxy.transport.core.connection;

import com.gllue.myproxy.transport.backend.BackendConnectionException;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
import com.gllue.myproxy.transport.exception.ServerErrorCode;

public class TooManyConnectionsException extends BackendConnectionException {
  private final int maxSize;

  public TooManyConnectionsException(final int maxCapacity) {
    this.maxSize = maxCapacity;
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return ServerErrorCode.ER_TOO_MANY_CONNECTIONS;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new Object[maxSize];
  }
}
