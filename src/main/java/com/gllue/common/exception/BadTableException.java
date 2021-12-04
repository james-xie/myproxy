package com.gllue.common.exception;

import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.exception.SQLErrorCode;

public class BadTableException extends BaseServerException {
  private final String table;

  public BadTableException(final String table) {
    this.table = table;
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_BAD_TABLE_ERROR;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new Object[] {table};
  }
}
