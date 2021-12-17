package com.gllue.metadata.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum TableType {
  PRIMARY(1),
  EXTENSION(2),

  PARTITION(3),
  STANDARD(4),

  TEMPORARY(5);

  private final int id;

  public static TableType getTableType(final int value) {
    for (var item : TableType.values()) {
      if (item.id == value) {
        return item;
      }
    }

    throw new IllegalArgumentException(String.format("Unknown table type id, [%d]", value));
  }
}
