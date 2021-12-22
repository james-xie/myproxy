package com.gllue.common;

import com.gllue.bootstrap.ServerContext;

public interface Initializer extends AutoCloseable {
  String name();

  void initialize(final ServerContext context);
}
