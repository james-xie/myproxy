package com.gllue.myproxy.common.exception;

import com.gllue.myproxy.transport.exception.SQLErrorCode;
import com.gllue.myproxy.transport.exception.ServerErrorCode;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseServerException extends RuntimeException {
  protected static final Object[] EMPTY_ERROR_MESSAGE_ARGS = new Object[0];

  public BaseServerException() {
    super();
  }

  public BaseServerException(Throwable cause) {
    super(cause);
  }

  public BaseServerException(String msg, Object... args) {
    super(String.format(msg, args));
  }

  public BaseServerException(Throwable cause, String msg, Object... args) {
    super(String.format(msg, args), cause);
  }

  public SQLErrorCode getErrorCode() {
    return ServerErrorCode.ER_SERVER_ERROR;
  }

  public Object[] getErrorMessageArgs() {
    return new Object[] {getMessage()};
  }

  public Throwable getRootCause() {
    Throwable throwable = this;
    int level = 10;
    do {
      Throwable cause = throwable.getCause();
      if (cause == null) {
        return throwable;
      }
      throwable = cause;
    } while (--level > 0);

    log.error("Unable to get root cause, an abnormal wrapped exception is found.", this);
    return throwable;
  }
}
