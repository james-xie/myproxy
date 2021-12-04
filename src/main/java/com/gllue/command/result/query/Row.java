package com.gllue.command.result.query;

public interface Row {

  /**
   * A meta data object describes the row data.
   *
   * @return query result meta data.
   */
  QueryResultMetaData getMetaData();

  /**
   * Get raw row data.
   *
   * @return raw row data
   */
  byte[][] getRowData();

  /**
   * Get raw value.
   *
   * @param columnIndex column index
   * @return raw value
   */
  byte[] getValue(int columnIndex);

  /**
   * Get value with the type declared in the meta data.
   *
   * @param columnIndex column index
   * @return typed value
   */
  <T> T getTypedValue(int columnIndex);
}
