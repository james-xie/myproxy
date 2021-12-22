package com.gllue.myproxy.transport.exception;

import com.gllue.myproxy.common.exception.BaseServerException;

public class UnsupportedCommandException extends TransportException {
  public UnsupportedCommandException(String msg, Object... args) {
    super(String.format(msg, args));
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_UNKNOWN_COM_ERROR;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return BaseServerException.EMPTY_ERROR_MESSAGE_ARGS;
  }
}
