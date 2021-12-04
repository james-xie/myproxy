package com.gllue.common.concurrent.executor;

import com.gllue.common.concurrent.ThreadPool;
import java.util.concurrent.ExecutorService;

/** A builder for direct executors. */
public final class DirectExecutorBuilder extends ExecutorBuilder {

  public DirectExecutorBuilder(final String name) {
    super(name);
  }

  @Override
  public ExecutorService build() {
    return ThreadPool.DIRECT_EXECUTOR_SERVICE;
  }


}
