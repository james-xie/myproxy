package com.gllue.command.handler.query;

import com.gllue.command.handler.CommandHandlerException;
import com.gllue.transport.exception.SQLErrorCode;
import com.gllue.transport.exception.ServerErrorCode;

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
