package com.gllue.transport.backend.connection;

import com.gllue.transport.backend.BackendConnectionException;
import com.gllue.transport.exception.SQLErrorCode;
import com.gllue.transport.exception.ServerErrorCode;

public class BackendConnectionClosedException extends BackendConnectionException {
  @Override
  public SQLErrorCode getErrorCode() {
    return ServerErrorCode.ER_LOST_BACKEND_CONNECTION;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return EMPTY_ERROR_MESSAGE_ARGS;
  }
}
