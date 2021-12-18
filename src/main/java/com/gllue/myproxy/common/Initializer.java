package com.gllue.myproxy.common;

import com.gllue.myproxy.bootstrap.ServerContext;

public interface Initializer extends AutoCloseable {
  void initialize(final ServerContext context);
}
