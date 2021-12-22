package com.gllue.myproxy.transport.backend.connection;

import com.gllue.myproxy.common.concurrent.ExtensibleFuture;
import javax.annotation.Nullable;

public interface BackendConnectionSupplier {
  @Nullable
  ExtensibleFuture<BackendConnection> getBackendConnection(String dataSource, @Nullable String database);
}
