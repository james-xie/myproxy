package com.gllue.transport.exception;

import com.gllue.common.exception.BaseServerException;

public abstract class TransportException extends BaseServerException {
  public TransportException() {
    super();
  }

  public TransportException(Throwable cause) {
    super(cause);
  }

  public TransportException(String msg, Object... args) {
    super(String.format(msg, args));
  }

  public TransportException(Throwable cause, String msg, Object... args) {
    super(String.format(msg, args), cause);
  }
}
