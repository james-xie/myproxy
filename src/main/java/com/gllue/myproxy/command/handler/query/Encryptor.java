package com.gllue.myproxy.command.handler.query;

public interface Encryptor {
  /** Wrap the expression with encryption function. */
  String encryptExpr(String expr);
}
