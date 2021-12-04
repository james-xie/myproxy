package com.gllue.common.exception;

import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.exception.SQLErrorCode;

public class ColumnExistsException extends BaseServerException {
  private final String column;

  public ColumnExistsException(final String column) {
    this.column = column;
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_DUP_FIELDNAME;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new Object[] {column};
  }
}
