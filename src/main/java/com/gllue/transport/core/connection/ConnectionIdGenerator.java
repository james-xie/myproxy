package com.gllue.transport.core.connection;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Connection ID generator.
 */
@NoArgsConstructor(access = AccessLevel.NONE)
public final class ConnectionIdGenerator {

  private int currentId;

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