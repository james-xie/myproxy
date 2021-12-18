package com.gllue.myproxy.command.handler.query.dml.select;

import com.gllue.myproxy.command.handler.CommandHandlerException;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
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
