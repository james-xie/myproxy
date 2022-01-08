package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.command.result.query.QueryResult;
import com.gllue.myproxy.common.Callback;
import java.util.concurrent.Executor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class QueryHandlerResult implements HandlerResult {
  public static final QueryHandlerResult OK_RESULT = new QueryHandlerResult();
  public static final QueryHandlerResult DIRECT_TRANSFERRED_RESULT =
      new QueryHandlerResult(0, 0, 0, null, true);

  private final long affectedRows;
  private final long lastInsertId;
  private final int warnings;
  private final QueryResult queryResult;
  private final boolean directTransferred;

  public QueryHandlerResult() {
    this(0, 0, 0);
  }

  public QueryHandlerResult(QueryResult queryResult) {
    this(0, queryResult);
  }

  public QueryHandlerResult(int warnings, QueryResult queryResult) {
    this(0, 0, warnings, queryResult, false);
  }

  public QueryHandlerResult(final long affectedRows, final long lastInsertId, final int warnings) {
    this(affectedRows, lastInsertId, warnings, null, false);
  }

  @Override
  public boolean isDirectTransferred() {
    return directTransferred;
  }

  public static Callback<CommandResult> wrappedCallbackWithDirectTransferredResult(
      Callback<HandlerResult> callback, Executor executor) {
    return new Callback<>() {
      @Override
      public void onSuccess(CommandResult result) {
        callback.onSuccess(DIRECT_TRANSFERRED_RESULT);
      }

      @Override
      public void onFailure(Throwable e) {
        callback.onSuccess(DIRECT_TRANSFERRED_RESULT);
      }

      @Override
      public Executor executor() {
        return executor;
      }
    };
  }
}
