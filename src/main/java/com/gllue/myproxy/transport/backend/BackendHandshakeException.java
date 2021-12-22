package com.gllue.myproxy.transport.backend;

import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
import com.gllue.myproxy.transport.exception.TransportException;

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
    return BaseServerException.EMPTY_ERROR_MESSAGE_ARGS;
  }
}
