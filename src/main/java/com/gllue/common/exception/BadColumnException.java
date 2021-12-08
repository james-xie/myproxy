package com.gllue.common.exception;

import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.exception.SQLErrorCode;

public class BadColumnException extends BaseServerException {
  private final String table;
  private final String column;

  public BadColumnException(final String table, final String column) {
    super("table: %s, column: %s", table, column);
    this.table = table;
    this.column = column;
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_BAD_FIELD_ERROR;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new Object[] {column, table};
  }
}
