package com.gllue.myproxy.common.properties;

public interface TypedPropertyValueConvertor<T> {
  T convert(String value);
}
