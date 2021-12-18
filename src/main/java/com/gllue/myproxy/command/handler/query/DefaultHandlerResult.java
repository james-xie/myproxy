package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.command.handler.HandlerResult;

public class DefaultHandlerResult implements HandlerResult {
  private static final DefaultHandlerResult INSTANCE = new DefaultHandlerResult();

  public static DefaultHandlerResult getInstance() {
    return INSTANCE;
  }
}
