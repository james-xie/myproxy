package com.gllue.myproxy.command.handler.query.dcl.set;

import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BadVariableValueException extends BaseServerException {
  private final String variable;
  private final String value;

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_WRONG_VALUE_FOR_VAR;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new String[] {variable, value};
  }
}
