package com.gllue.command.handler.query;

import com.gllue.command.handler.HandlerResult;

public class DirectTransferredResult implements HandlerResult {
  public static final DirectTransferredResult INSTANCE = new DirectTransferredResult();

  @Override
  public boolean isDirectTransferred() {
    return true;
  }
}
