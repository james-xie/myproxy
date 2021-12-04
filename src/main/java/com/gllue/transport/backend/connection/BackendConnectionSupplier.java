package com.gllue.transport.backend.connection;

import com.gllue.common.concurrent.ExtensibleFuture;
import javax.annotation.Nullable;

public interface BackendConnectionSupplier {
  @Nullable
  ExtensibleFuture<BackendConnection> getBackendConnection(String dataSource, @Nullable String database);
}
