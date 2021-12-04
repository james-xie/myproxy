package com.gllue.common.properties;

import com.gllue.common.exception.BaseServerException;
import com.gllue.transport.exception.SQLErrorCode;
import com.gllue.transport.exception.ServerErrorCode;

public final class TypedPropertyValueException extends BaseServerException {
  public TypedPropertyValueException(final String value, final Class<?> type) {
    this(value, type.getSimpleName());
  }

  public TypedPropertyValueException(final String value, final String typeName) {
    super(String.format("Value `%s` cannot convert to type `%s`.", value, typeName));
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
