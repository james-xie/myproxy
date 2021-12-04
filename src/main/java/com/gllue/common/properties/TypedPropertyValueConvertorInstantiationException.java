package com.gllue.common.properties;

import com.gllue.common.exception.BaseServerException;
import com.gllue.transport.exception.SQLErrorCode;
import com.gllue.transport.exception.ServerErrorCode;

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
