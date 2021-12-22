package com.gllue.myproxy.config;

import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.exception.ServerErrorCode;
import com.gllue.myproxy.transport.exception.SQLErrorCode;

public class ConfigurationException extends BaseServerException {
  public ConfigurationException(Throwable cause) {
    super(cause);
  }

  public ConfigurationException(String msg, Object... args) {
    super(String.format(msg, args));
  }

  public ConfigurationException(Throwable cause, String msg, Object... args) {
    super(String.format(msg, args), cause);
  }

  @Override
  public SQLErrorCode getErrorCode() {
    return ServerErrorCode.ER_BAD_CONFIGURATION;
  }

  @Override
  public Object[] getErrorMessageArgs() {
    return EMPTY_ERROR_MESSAGE_ARGS;
  }
}
