package com.gllue.common.generator;

import com.gllue.common.exception.BaseServerException;

public class InvalidSystemClock extends BaseServerException {
  public InvalidSystemClock(String msg, Object... args) {
    super(String.format(msg, args));
  }
}
