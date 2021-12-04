package com.gllue.common.exception;

import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.exception.SQLErrorCode;

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
