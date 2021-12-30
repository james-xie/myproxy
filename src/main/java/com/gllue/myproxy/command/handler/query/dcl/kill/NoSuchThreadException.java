package com.gllue.myproxy.command.handler.query.dcl.kill;

import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class NoSuchThreadException extends BaseServerException {
  private final int threadId;

  @Override
  public SQLErrorCode getErrorCode() {
    return MySQLServerErrorCode.ER_NO_SUCH_THREAD;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return new Object[] {threadId};
  }
}
