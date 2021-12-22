package com.gllue.myproxy.transport.backend.datasource;

import com.gllue.myproxy.transport.exception.ServerErrorCode;
import com.gllue.myproxy.transport.backend.BackendConnectionException;
import com.gllue.myproxy.transport.exception.SQLErrorCode;

public class TooManyBackendConnectionException extends BackendConnectionException {
  private final int maxCapacity;

  public TooManyBackendConnectionException(final int maxCapacity) {
    this.maxCapacity = maxCapacity;
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return ServerErrorCode.ER_TOO_MANY_BACKEND_CONNECTION;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new Object[maxCapacity];
  }
}
