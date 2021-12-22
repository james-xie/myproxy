package com.gllue.myproxy.common.concurrent.executor;

import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
import com.gllue.myproxy.transport.exception.ServerErrorCode;


public class ExecutorRejectedExecutionException extends BaseServerException {
  public ExecutorRejectedExecutionException(Throwable cause) {
    super(cause);
  }

  public ExecutorRejectedExecutionException(String msg, Object... args) {
    super(String.format(msg, args));
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return ServerErrorCode.ER_TOO_MANY_EXECUTION_TASK;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return EMPTY_ERROR_MESSAGE_ARGS;
  }
}
