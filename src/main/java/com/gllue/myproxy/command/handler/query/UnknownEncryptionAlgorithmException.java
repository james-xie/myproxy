package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.common.exception.BaseServerException;

public class UnknownEncryptionAlgorithmException extends BaseServerException {
  public UnknownEncryptionAlgorithmException(final String name) {
    super(name);
  }
}
