package com.gllue.myproxy.transport.backend.connection;

import com.gllue.myproxy.transport.exception.ServerErrorCode;
import com.gllue.myproxy.transport.backend.BackendConnectionException;
import com.gllue.myproxy.transport.exception.SQLErrorCode;

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
