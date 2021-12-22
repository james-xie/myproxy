package com.gllue.myproxy.common.concurrent.executor;

import java.util.concurrent.ThreadPoolExecutor;
import lombok.extern.slf4j.Slf4j;

/**
 * A handler for rejected tasks that adds the specified element to this queue, waiting if necessary for space to become
 * available.
 */
@Slf4j
public class WaitingQueueAvailablePolicy implements AccountableRejectedExecutionHandler {

  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
    try {
      log.warn(
          "Try to put an execution into the queue again, it will waiting for the queue space become available.");
      executor.getQueue().put(r);
    } catch (InterruptedException e) {
      throw new ExecutorRejectedExecutionException(e);
    }
  }

  @Override
  public long rejectedExecutions() {
    return 0;
  }
}
