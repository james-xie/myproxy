package com.gllue.common.properties;

public interface TypedPropertyValueConvertor<T> {
  T convert(String value);
}
