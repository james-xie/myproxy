package com.gllue.myproxy.transport.frontend.connection;

import com.gllue.myproxy.transport.core.connection.Connection;

public class SessionContextImpl implements SessionContext {
  private Connection connection;
  private String encryptKey;

  public SessionContextImpl(Connection connection) {
    this.connection = connection;
  }

  @Override
  public boolean isTransactionOpened() {
    if (connection == null) {
      throw new IllegalStateException("Session context has already closed.");
    }
    return connection.isTransactionOpened();
  }

  @Override
  public String getEncryptKey() {
    return encryptKey;
  }

  @Override
  public void setEncryptKey(String encryptKey) {
    this.encryptKey = encryptKey;
  }

  @Override
  public void close() throws Exception {
    this.connection = null;
    this.encryptKey = null;
  }
}
