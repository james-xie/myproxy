package com.gllue.myproxy.transport.core.connection;

import com.gllue.myproxy.transport.exception.TransportException;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ConnectionException extends TransportException {
  public ConnectionException(Throwable cause) {
    super(cause);
  }
}
