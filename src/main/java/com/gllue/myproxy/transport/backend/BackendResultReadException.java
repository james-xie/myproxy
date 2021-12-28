package com.gllue.myproxy.transport.backend;

import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
import com.gllue.myproxy.transport.exception.TransportException;

public class BackendResultReadException extends TransportException {
  private final SQLErrorCode sqlErrorCode;

  public BackendResultReadException(final SQLErrorCode sqlErrorCode) {
    super(
        "errCode: " + sqlErrorCode.getErrorCode() + ", errMsg: " + sqlErrorCode.getErrorMessage());
    this.sqlErrorCode = sqlErrorCode;
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return sqlErrorCode;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return BaseServerException.EMPTY_ERROR_MESSAGE_ARGS;
  }
}
