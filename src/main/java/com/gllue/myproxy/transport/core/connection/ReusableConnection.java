package com.gllue.myproxy.transport.core.connection;

public interface ReusableConnection extends Connection {
  void assign();

  boolean isAssigned();

  boolean release();

  boolean isReleased();

  void releaseOrClose();
}
