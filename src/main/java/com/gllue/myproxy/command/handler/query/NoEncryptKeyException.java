package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.command.handler.CommandHandlerException;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
import com.gllue.myproxy.transport.exception.ServerErrorCode;

public class NoEncryptKeyException extends CommandHandlerException {

  @Override
  public SQLErrorCode getErrorCode() {
    return ServerErrorCode.ER_NO_ENCRYPT_KEY;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return EMPTY_ERROR_MESSAGE_ARGS;
  }
}
