package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.command.handler.HandlerResult;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class DefaultHandlerResult implements HandlerResult {
  private static final DefaultHandlerResult INSTANCE = new DefaultHandlerResult(0);

  public static DefaultHandlerResult getInstance() {
    return INSTANCE;
  }

  private final long warnings;

  @Override
  public boolean isDirectTransferred() {
    return false;
  }
}
