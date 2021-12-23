package com.gllue.myproxy.command.handler.query.dml.select;

import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.result.query.QueryResult;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class SelectQueryResult implements HandlerResult {
  public static final SelectQueryResult DIRECT_TRANSFERRED_RESULT =
      new SelectQueryResult(true, 0, null);

  private final boolean directTransferred;
  private final long warnings;
  private final QueryResult queryResult;
}
