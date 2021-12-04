package com.gllue.config;

import static com.gllue.config.GenericConfigPropertyKey.availableProcessors;
import com.gllue.common.properties.TypedPropertyKey;
import com.gllue.common.properties.TypedPropertyValue.Type;
import lombok.Getter;

/** Typed property key of transport configuration. */
@Getter
public enum TransportConfigPropertyKey implements TypedPropertyKey {
  FRONTEND_SERVER_ADDRESS("frontend.server_address", "localhost", Type.STRING),

  FRONTEND_SERVER_PORT("frontend.server_port", 13306, Type.INTEGER),

  FRONTEND_WORKER_COUNT("frontend.worker_count", availableProcessors(), Type.INTEGER),

  FRONTEND_BACKLOG("frontend.backlog", 50, Type.INTEGER),

  FRONTEND_WRITE_BUFFER_LOW_WATER_MARK(
      "frontend.write_buffer_low_water_mark", 8 * 1024 * 1024, Type.INTEGER),

  FRONTEND_WRITE_BUFFER_HIGH_WATER_MARK(
      "frontend.write_buffer_high_water_mark", 16 * 1024 * 1024, Type.INTEGER),


  BACKEND_WORKER_COUNT("backend.worker_count", availableProcessors(), Type.INTEGER),

  BACKEND_CONNECT_TIMEOUT_MILLIS("backend.connect_timeout_millis", 10 * 1000, Type.INTEGER),

  BACKEND_WRITE_BUFFER_LOW_WATER_MARK(
      "backend.write_buffer_low_water_mark", 8 * 1024 * 1024, Type.INTEGER),

  BACKEND_WRITE_BUFFER_HIGH_WATER_MARK(
      "backend.write_buffer_high_water_mark", 16 * 1024 * 1024, Type.INTEGER);

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
