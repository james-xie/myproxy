package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.command.handler.HandlerResult;

public class DirectTransferredResult implements HandlerResult {
  public static final DirectTransferredResult INSTANCE = new DirectTransferredResult();

  @Override
  public boolean isDirectTransferred() {
    return true;
  }
}
