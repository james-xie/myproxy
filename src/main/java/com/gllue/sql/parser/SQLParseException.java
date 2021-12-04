package com.gllue.sql.parser;

import com.gllue.common.exception.BaseServerException;
import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.exception.SQLErrorCode;

public class SQLParseException extends BaseServerException {
  public SQLParseException(String msg, Object... args) {
    super(msg, args);
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_PARSE_ERROR;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new Object[] {getMessage()};
  }
}
