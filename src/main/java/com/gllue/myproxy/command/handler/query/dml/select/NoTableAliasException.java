package com.gllue.myproxy.command.handler.query.dml.select;

import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.exception.SQLErrorCode;

public class NoTableAliasException extends BaseServerException {

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_DERIVED_MUST_HAVE_ALIAS;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return EMPTY_ERROR_MESSAGE_ARGS;
  }
}
