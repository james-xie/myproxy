package com.gllue.command.handler.query;

import com.gllue.command.handler.HandlerResult;

public class DefaultHandlerResult implements HandlerResult {
  private static final DefaultHandlerResult INSTANCE = new DefaultHandlerResult();

  public static DefaultHandlerResult getInstance() {
    return INSTANCE;
  }
}
