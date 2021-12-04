package com.gllue.transport.core.connection;

public abstract class WritabilityChangedListener<T extends Connection>
    implements TransportListener<T> {
  @Override
  public void onFailure(Throwable e) {

  }
}
