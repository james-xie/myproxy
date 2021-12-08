package com.gllue.command.handler.query.dml.select;

import com.gllue.command.handler.CommandHandlerException;
import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.exception.SQLErrorCode;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class NotUniqueTableOrAliasException extends CommandHandlerException {
  private final String tableOrAlias;

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_ER_NONUNIQ_TABLE;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new String[] {tableOrAlias};
  }
}
