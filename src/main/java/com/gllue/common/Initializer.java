package com.gllue.common;

import com.gllue.bootstrap.ServerContext;

public interface Initializer extends AutoCloseable {
  void initialize(final ServerContext context);
}
