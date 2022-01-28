package com.gllue.myproxy.repository.zookeeper;

import com.gllue.myproxy.common.properties.TypedPropertyKey;
import com.gllue.myproxy.common.properties.TypedPropertyValue.Type;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** Typed property key of generic configuration. */
@Getter
@RequiredArgsConstructor
public enum ZookeeperConfigPropertyKey implements TypedPropertyKey {
  ADDRESS("address", "127.0.0.1:2181", Type.STRING),
  SESSION_TIMEOUT_MS("session_timeout_in_mills", TimeUnit.SECONDS.toMillis(40), Type.INTEGER),
  OPERATION_TIMEOUT_MS("operation_timeout_in_mills", TimeUnit.SECONDS.toMillis(15), Type.INTEGER),
  CONNECT_TIMEOUT_MS("connect_timeout_in_mills", TimeUnit.SECONDS.toMillis(15), Type.INTEGER),

  RETRY_BASE_TIME_MS("retry.base_time_in_mills", TimeUnit.SECONDS.toMillis(3), Type.INTEGER),
  MAX_RETRIES("retry.max_retries", 3, Type.INTEGER),
  RETRY_MAX_TIME_MS("retry.max_time_in_mills", TimeUnit.SECONDS.toMillis(15), Type.INTEGER),
  ;

  private static final String PREFIX = "zookeeper";

  private final String key;

  private final String defaultValue;

  private final Type type;

  ZookeeperConfigPropertyKey(final String key, final Object defaultValue, final Type type) {
    this.key = key;
    this.defaultValue = String.valueOf(defaultValue);
    this.type = type;
  }

  @Override
  public String getPrefix() {
    return PREFIX;
  }
}
