package com.gllue.command.handler.query.dml.select;

import com.gllue.command.handler.CommandHandlerException;
import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.exception.SQLErrorCode;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class AmbiguousColumnException extends CommandHandlerException {
  private final String column;
  private final String location;

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_NON_UNIQ_ERROR;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new String[] {column, location};
  }
}
