package com.gllue.transport.backend;

import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.exception.SQLErrorCode;
import com.gllue.transport.exception.TransportException;

public class BackendHandshakeException extends TransportException {
  public BackendHandshakeException(Throwable cause) {
    super(cause);
  }

  public BackendHandshakeException(String msg, Object... args) {
    super(String.format(msg, args));
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_HANDSHAKE_ERROR;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return EMPTY_ERROR_MESSAGE_ARGS;
  }
}
