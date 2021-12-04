package com.gllue.common.exception;

import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.exception.SQLErrorCode;

public class TableExistsException extends BaseServerException {
  private final String table;

  public TableExistsException(final String table) {
    this.table = table;
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_TABLE_EXISTS_ERROR;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new Object[] {table};
  }

}
