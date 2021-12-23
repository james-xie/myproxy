package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.command.handler.CommandHandlerException;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
import com.gllue.myproxy.transport.exception.ServerErrorCode;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BadEncryptKeyException extends CommandHandlerException {
  private final String badKey;

  @Override
  public SQLErrorCode getErrorCode() {
    return ServerErrorCode.ER_BAD_ENCRYPT_KEY;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new String[] {badKey};
  }
}
