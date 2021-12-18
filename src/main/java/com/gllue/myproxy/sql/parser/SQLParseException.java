package com.gllue.myproxy.sql.parser;

import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.exception.SQLErrorCode;

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
