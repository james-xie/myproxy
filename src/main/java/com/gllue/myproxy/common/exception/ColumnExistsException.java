package com.gllue.myproxy.common.exception;

import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.exception.SQLErrorCode;

public class ColumnExistsException extends BaseServerException {
  private final String column;

  public ColumnExistsException(final String column) {
    super(column);
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
