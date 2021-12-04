package com.gllue.common.concurrent.executor;

import java.util.concurrent.RejectedExecutionHandler;

public interface AccountableRejectedExecutionHandler extends RejectedExecutionHandler {
  /**
   * The number of rejected executions.
   */
  long rejectedExecutions();
}
