package com.gllue.transport.backend.connection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface BackendConnectionListener {
  boolean onConnected(@Nonnull BackendConnection connection);

  void onConnectFailed(Exception e);

  void onClosed(@Nullable BackendConnection connection);
}
