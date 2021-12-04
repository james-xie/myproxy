package com.gllue.command.handler;

import com.gllue.common.Callback;
import com.gllue.common.concurrent.AbstractRunnable;
import com.gllue.common.concurrent.ThreadPool;
import com.gllue.common.concurrent.ThreadPool.Name;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

public class HandlerExecutor {
  private final ThreadPool threadPool;

  public HandlerExecutor(final ThreadPool threadPool) {
    this.threadPool = threadPool;
  }

  public <Request extends HandlerRequest, Result extends HandlerResult> void execute(
      CommandHandler<Request, Result> handler, Request request, Callback<Result> callback) {
    threadPool.executor(Name.COMMAND).submit(new HandlerRunner<>(handler, request, callback));
  }

  @Slf4j
  @RequiredArgsConstructor
  static class HandlerRunner<Request extends HandlerRequest, Result extends HandlerResult>
      extends AbstractRunnable {
    private final CommandHandler<Request, Result> handler;
    private final Request request;
    private final Callback<Result> callback;

    @Override
    protected void doRun() throws Exception {
      handler.execute(request, callback);
    }

    @Override
    public void onFailure(Exception e) {
      if (log.isDebugEnabled()) {
        log.error("Failed to run command handler. [request={}, handler={}]", request, handler);
      }
      callback.onFailure(e);
    }
  }
}
