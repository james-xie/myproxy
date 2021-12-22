package com.gllue.myproxy.command.handler.query.dml.insert;

import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ColumnCountNotMatchValueCountException extends BaseServerException {
  private final int rowNum;

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_WRONG_VALUE_COUNT_ON_ROW;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new String[] {String.valueOf(rowNum)};
  }
}
