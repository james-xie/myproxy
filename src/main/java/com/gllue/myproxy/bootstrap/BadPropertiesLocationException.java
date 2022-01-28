package com.gllue.myproxy.bootstrap;

import com.gllue.myproxy.common.exception.BaseServerException;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BadPropertiesLocationException extends BaseServerException {
  public BadPropertiesLocationException(final String location) {
    super("Bad properties location. [%s]", location);
  }
}
