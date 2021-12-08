package com.gllue.common.exception;

import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.exception.SQLErrorCode;

public class NoDatabaseException extends BaseServerException{
  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_NO_DB_ERROR;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return EMPTY_ERROR_MESSAGE_ARGS;
  }
}
