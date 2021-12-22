package com.gllue.myproxy.common.exception;

import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.exception.SQLErrorCode;

public class DatabaseExistsException extends BaseServerException {
  private final String database;

  public DatabaseExistsException(final String database) {
    this.database = database;
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_DB_CREATE_EXISTS;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new Object[] {database};
  }
}
