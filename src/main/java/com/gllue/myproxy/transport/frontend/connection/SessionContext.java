package com.gllue.myproxy.transport.frontend.connection;

public interface SessionContext extends AutoCloseable {
  boolean isTransactionOpened();

  String getEncryptKey();

  void setEncryptKey(String encryptKey);
}
