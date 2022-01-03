package com.gllue.myproxy.transport.core.connection;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Connection ID generator. */
public final class ConnectionIdGenerator {

  private int currentId;

  public ConnectionIdGenerator() {
    this(0);
  }

  public ConnectionIdGenerator(final int initId) {
    Preconditions.checkArgument(initId >= 0);
    this.currentId = initId;
  }

  /**
   * Get next connection ID.
   *
   * @return next connection ID
   */
  public synchronized int nextId() {
    if (currentId >= Integer.MAX_VALUE) {
      currentId = 0;
    }
    return ++currentId;
  }
}
