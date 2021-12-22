package com.gllue.myproxy.common.exception;

import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.exception.SQLErrorCode;

public class BadDatabaseException extends BaseServerException{
  private final String database;

  public BadDatabaseException(final String database) {
    this.database = database;
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_BAD_DB_ERROR;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new Object[] {database};
  }
}
