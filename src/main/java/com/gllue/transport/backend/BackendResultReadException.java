package com.gllue.transport.backend;

import com.gllue.transport.exception.SQLErrorCode;
import com.gllue.transport.exception.TransportException;
import lombok.Getter;

public class BackendResultReadException extends TransportException {
  private final SQLErrorCode sqlErrorCode;

  public BackendResultReadException(final SQLErrorCode sqlErrorCode) {
    this.sqlErrorCode = sqlErrorCode;
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return sqlErrorCode;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return EMPTY_ERROR_MESSAGE_ARGS;
  }
}
