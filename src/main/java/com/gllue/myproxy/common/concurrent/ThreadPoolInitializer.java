package com.gllue.myproxy.common.concurrent;

import com.gllue.myproxy.bootstrap.ServerContext;
import com.gllue.myproxy.common.Initializer;

public class ThreadPoolInitializer implements Initializer {
  private ThreadPool threadPool;

  @Override
  public String name() {
    return "thread pool";
  }

  @Override
  public void initialize(ServerContext context) {
    threadPool = new ThreadPool(context.getConfigurations());
    context.setThreadPool(threadPool);
  }

  @Override
  public void close() throws Exception {
    if (threadPool != null) threadPool.shutdown();
  }
}
