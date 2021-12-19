package com.gllue.command.handler.query.dml.insert;

import com.gllue.common.exception.BaseServerException;
import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.exception.SQLErrorCode;
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
