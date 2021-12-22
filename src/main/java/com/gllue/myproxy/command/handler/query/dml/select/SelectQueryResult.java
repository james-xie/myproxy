package com.gllue.myproxy.command.handler.query.dml.select;

import com.gllue.myproxy.command.handler.HandlerResult;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SelectQueryResult implements HandlerResult {
  public static final SelectQueryResult DIRECT_TRANSFERRED_RESULT = new SelectQueryResult(true);

  private boolean directTransferred;

  @Override
  public boolean isDirectTransferred() {
    return directTransferred;
  }
}
