package com.gllue.myproxy.command.result.query;

public interface QueryResult {
  /**
   * Iterate next row.
   *
   * @return has next row
   */
  boolean next();

  /**
   * Get raw value.
   *
   * @param columnIndex column index
   * @return raw value
   */
  byte[] getValue(int columnIndex);

  /**
   * Get value as string.
   *
   * @param columnIndex column index
   * @return string value
   */
  String getStringValue(int columnIndex);

  /**
   * Get the column definitions which is the meta data of the columns in the query result.
   *
   * @return the meta data describes columns
   */
  QueryResultMetaData getMetaData();

  /** Close the query result, release the relevant resources. */
  void close();
}
