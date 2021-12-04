package com.gllue.command.handler.query;

import com.gllue.command.handler.CommandHandlerException;
import com.gllue.transport.exception.SQLErrorCode;
import com.gllue.transport.exception.ServerErrorCode;

public class BadSQLException extends CommandHandlerException {
  public BadSQLException(String msg, Object... args) {
    super(msg, args);
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return ServerErrorCode.ER_BAD_SQL;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new Object[] {getMessage()};
  }
}
