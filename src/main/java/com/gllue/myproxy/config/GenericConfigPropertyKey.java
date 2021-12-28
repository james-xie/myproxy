package com.gllue.myproxy.config;

import com.gllue.myproxy.common.properties.TypedPropertyKey;
import com.gllue.myproxy.common.properties.TypedPropertyValue.Type;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** Typed property key of generic configuration. */
@Getter
@RequiredArgsConstructor
public enum GenericConfigPropertyKey implements TypedPropertyKey {
  PROCESSORS("processors", availableProcessors(), Type.INTEGER),

  // data source
  DATA_SOURCE_CONFIGS("data_source.configs", "", Type.LIST_OF_STRING),

  // thread pool
  THREAD_POOL_FIXED_EXECUTOR_QUEUE_SIZE(
      "thread_pool.executor.fixed.queue_size", 1000, Type.INTEGER),

  // table
  EXTENSION_TABLE_MAX_COLUMNS_PER_TABLE(
      "table.partition.extension_table.max_columns_per_table", 150, Type.INTEGER),
  EXTENSION_TABLE_COLUMNS_ALLOCATION_WATERMARK(
      "table.partition.extension_table.columns_allocation_watermark", 0.9, Type.DOUBLE),

  // query result
  QUERY_RESULT_CACHED_MAX_CAPACITY_IN_BYTES(
      "query.result.cached.max_capacity_in_bytes", 100 * 1024 * 1024, Type.INTEGER),

  // encryption
  ENCRYPTION_ALGORITHM("encryption.algorithm", "AES", Type.STRING),

  // repository
  REPOSITORY_ROOT_PATH("repository.root_path", "/myproxy", Type.STRING);

  private static final String PREFIX = "generic";

  private final String key;

  private final String defaultValue;

  private final Type type;

  GenericConfigPropertyKey(final String key, final Object defaultValue, final Type type) {
    this.key = key;
    this.defaultValue = String.valueOf(defaultValue);
    this.type = type;
  }

  @Override
  public String getPrefix() {
    return PREFIX;
  }

  public static int availableProcessors() {
    return Math.min(32, Runtime.getRuntime().availableProcessors());
  }

  public static int halfNumberOfAvailableProcessors() {
    return Math.max(1, availableProcessors() / 2);
  }

  public static int twiceNumberOfAvailableProcessors() {
    return availableProcessors() * 2;
  }
}
