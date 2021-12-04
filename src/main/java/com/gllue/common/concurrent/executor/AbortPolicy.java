package com.gllue.common.concurrent.executor;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * If an execution is rejected, an exception is thrown by the handler.
 */
public class AbortPolicy implements AccountableRejectedExecutionHandler {

  private final AtomicLong rejectedCount = new AtomicLong();

  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
    rejectedCount.incrementAndGet();
    throw new ExecutorRejectedExecutionException(
        "Rejected execution of {} on {}. [isShutdown: {}]", r, executor, executor.isShutdown());
  }

  @Override
  public long rejectedExecutions() {
    return rejectedCount.get();
  }
}
