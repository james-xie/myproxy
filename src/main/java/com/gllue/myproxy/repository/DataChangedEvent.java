package com.gllue.myproxy.repository;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Data changed event.
 */
@RequiredArgsConstructor
@Getter
public final class DataChangedEvent {

  private final String key;

  private final Object value;

  private final Type type;

  /**
   * Data changed type.
   */
  public enum Type {
    CREATED, UPDATED, DELETED, IGNORED
  }
}
