package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.command.result.query.QueryResult;
import com.gllue.myproxy.common.Callback;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class WrappedHandlerResult implements HandlerResult {
  private final CommandResult commandResult;

  @Override
  public long getAffectedRows() {
    return commandResult.getAffectedRows();
  }

  @Override
  public long getLastInsertId() {
    return commandResult.getLastInsertId();
  }

  @Override
  public int getWarnings() {
    return commandResult.getWarnings();
  }

  @Override
  public QueryResult getQueryResult() {
    return commandResult.getQueryResult();
  }

  @Override
  public boolean isDirectTransferred() {
    return false;
  }

  public static Callback<CommandResult> wrappedCallback(Callback<HandlerResult> callback) {
    return new Callback<>() {
      @Override
      public void onSuccess(CommandResult result) {
        callback.onSuccess(new WrappedHandlerResult(result));
      }

      @Override
      public void onFailure(Throwable e) {
        callback.onFailure(e);
      }
    };
  }
}
