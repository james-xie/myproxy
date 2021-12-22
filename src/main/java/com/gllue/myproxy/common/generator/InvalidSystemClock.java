package com.gllue.myproxy.common.generator;

import com.gllue.myproxy.common.exception.BaseServerException;

public class InvalidSystemClock extends BaseServerException {
  public InvalidSystemClock(String msg, Object... args) {
    super(String.format(msg, args));
  }
}
