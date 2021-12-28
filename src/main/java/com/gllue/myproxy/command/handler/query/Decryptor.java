package com.gllue.myproxy.command.handler.query;

public interface Decryptor {
  /** Wrap the expression with decryption function. */
  String decryptExpr(String expr);
}
