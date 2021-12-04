package com.gllue.transport.backend.datasource;

import com.gllue.common.concurrent.ExtensibleFuture;
import com.gllue.transport.core.connection.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public final class DataSourceManager<T extends DataSource<C>, C extends Connection> {
  private final Map<String, T> dataSources;

  public DataSourceManager(List<T> dataSources) {
    this.dataSources =
        Collections.unmodifiableMap(
            dataSources.stream().collect(Collectors.toMap(DataSource::getName, (x) -> x)));
  }

  @Nullable
  public T getDataSource(final String dataSourceName) {
    return dataSources.get(dataSourceName);
  }

  @Nullable
  public ExtensibleFuture<C> getBackendConnection(
      final String dataSourceName, @Nullable final String database) {
    var dataSource = dataSources.get(dataSourceName);
    if (dataSource == null) {
      return null;
    }
    return dataSource.tryAcquireConnection(database);
  }
}
