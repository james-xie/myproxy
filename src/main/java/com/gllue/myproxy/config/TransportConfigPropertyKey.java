package com.gllue.myproxy.config;

import static com.gllue.myproxy.transport.core.connection.AbstractConnectionPool.DEFAULT_IDLE_TIMEOUT_IN_MILLS;
import static com.gllue.myproxy.transport.core.connection.AbstractConnectionPool.DEFAULT_KEEP_ALIVE_QUERY_TIMEOUT_IN_MILLS;
import static com.gllue.myproxy.transport.core.connection.AbstractConnectionPool.DEFAULT_KEEP_ALIVE_TIME_IN_MILLS;
import static com.gllue.myproxy.transport.core.connection.AbstractConnectionPool.DEFAULT_MAX_LIFE_TIME_IN_MILLS;

import com.gllue.myproxy.common.properties.TypedPropertyKey;
import com.gllue.myproxy.common.properties.TypedPropertyValue.Type;
import java.util.concurrent.TimeUnit;
import lombok.Getter;

/** Typed property key of transport configuration. */
@Getter
public enum TransportConfigPropertyKey implements TypedPropertyKey {
  FRONTEND_SERVER_ADDRESS("frontend.server_address", "localhost", Type.STRING),

  FRONTEND_SERVER_PORT("frontend.server_port", 13306, Type.INTEGER),

  FRONTEND_WORKER_COUNT(
      "frontend.worker_count", GenericConfigPropertyKey.availableProcessors(), Type.INTEGER),

  FRONTEND_BACKLOG("frontend.backlog", 500, Type.INTEGER),

  FRONTEND_WRITE_BUFFER_LOW_WATER_MARK(
      "frontend.write_buffer_low_water_mark", 8 * 1024 * 1024, Type.INTEGER),

  FRONTEND_WRITE_BUFFER_HIGH_WATER_MARK(
      "frontend.write_buffer_high_water_mark", 16 * 1024 * 1024, Type.INTEGER),

  FRONTEND_CONNECTION_MAX_IDLE_TIME_IN_MILLS(
      "frontend.connection.max_idle_time_in_mills", TimeUnit.HOURS.toMillis(8), Type.LONG),

  FRONTEND_CONNECTION_IDLE_DETECT_INTERVAL_IN_MILLS(
      "frontend.connection.idle_detect_interval_in_mills", TimeUnit.MINUTES.toMillis(1), Type.LONG),

  BACKEND_WORKER_COUNT(
      "backend.worker_count", GenericConfigPropertyKey.availableProcessors(), Type.INTEGER),

  BACKEND_CONNECT_TIMEOUT_MILLIS("backend.connect_timeout_millis", 10 * 1000, Type.INTEGER),

  BACKEND_WRITE_BUFFER_LOW_WATER_MARK(
      "backend.write_buffer_low_water_mark", 8 * 1024 * 1024, Type.INTEGER),

  BACKEND_WRITE_BUFFER_HIGH_WATER_MARK(
      "backend.write_buffer_high_water_mark", 16 * 1024 * 1024, Type.INTEGER),

  BACKEND_CONNECTION_POOL_SIZE("backend.connection.connection_pool_size", 1000, Type.INTEGER),

  BACKEND_CONNECTION_IDLE_TIMEOUT_IN_MILLS(
      "backend.connection.idle_timeout_in_mills", DEFAULT_IDLE_TIMEOUT_IN_MILLS, Type.LONG),

  BACKEND_CONNECTION_KEEP_ALIVE_TIME_IN_MILLS(
      "backend.connection.keep_alive_time_in_mills", DEFAULT_KEEP_ALIVE_TIME_IN_MILLS, Type.LONG),

  BACKEND_CONNECTION_KEEP_ALIVE_QUERY_TIMEOUT_IN_MILLS(
      "backend.connection.keep_alive_query_timeout_in_mills",
      DEFAULT_KEEP_ALIVE_QUERY_TIMEOUT_IN_MILLS,
      Type.LONG),

  BACKEND_CONNECTION_MAX_LIFE_TIME_IN_MILLS(
      "backend.connection.max_life_time_in_mills", DEFAULT_MAX_LIFE_TIME_IN_MILLS, Type.LONG),
  ;

  private static final String PREFIX = "transport";

  private final String key;

  private final String defaultValue;

  private final Type type;

  TransportConfigPropertyKey(final String key, final Object defaultValue, final Type type) {
    this.key = key;
    this.defaultValue = String.valueOf(defaultValue);
    this.type = type;
  }

  @Override
  public String getPrefix() {
    return PREFIX;
  }
}
