package com.gllue.myproxy.command.handler;

import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.AbstractRunnable;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.concurrent.ThreadPool.Name;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

public class HandlerExecutor {
  private final ThreadPool threadPool;

  public HandlerExecutor(final ThreadPool threadPool) {
    this.threadPool = threadPool;
  }

  public <Request extends HandlerRequest> void execute(
      CommandHandler<Request> handler, Request request, Callback<HandlerResult> callback) {
    threadPool.executor(Name.COMMAND).submit(new HandlerRunner<>(handler, request, callback));
  }

  @Slf4j
  @RequiredArgsConstructor
  static class HandlerRunner<Request extends HandlerRequest> extends AbstractRunnable {
    private final CommandHandler<Request> handler;
    private final Request request;
    private final Callback<HandlerResult> callback;

    @Override
    protected void doRun() throws Exception {
      handler.execute(request, callback);
    }

    @Override
    public void onFailure(Exception e) {
      if (log.isDebugEnabled()) {
        log.error("Failed to run command handler. [request={}, handler={}]", request, handler, e);
      }
      callback.onFailure(e);
    }
  }
}
