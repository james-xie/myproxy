package com.gllue.repository;

import com.gllue.common.exception.BaseServerException;

public class PersistRepositoryException extends BaseServerException {
  public PersistRepositoryException(Throwable cause) {
    super(cause);
  }

  public PersistRepositoryException(String msg, Object... args) {
    super(String.format(msg, args));
  }

  public PersistRepositoryException(Throwable cause, String msg, Object... args) {
    super(String.format(msg, args), cause);
  }


}
