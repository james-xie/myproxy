package com.gllue.common.concurrent;

import com.gllue.bootstrap.ServerContext;
import com.gllue.common.Initializer;

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
    threadPool.shutdown();
  }
}
