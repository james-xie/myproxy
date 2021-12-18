package com.gllue.myproxy.common.properties;

import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.exception.SQLErrorCode;
import com.gllue.myproxy.transport.exception.ServerErrorCode;

public class TypedPropertyValueConvertorInstantiationException extends BaseServerException {
  public TypedPropertyValueConvertorInstantiationException(Class<?> convertorClass) {
    super("Failed to instantiate convertor. [{}]", convertorClass.getSimpleName());
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
