package com.gllue.myproxy.common.exception;

import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.exception.SQLErrorCode;

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
