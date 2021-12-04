package com.gllue.transport.backend.datasource;

import com.gllue.transport.backend.BackendConnectionException;
import com.gllue.transport.exception.SQLErrorCode;
import com.gllue.transport.exception.ServerErrorCode;

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
