package com.gllue.myproxy.command.result.query;

import com.gllue.myproxy.transport.constant.MySQLColumnType;

/**
 * A meta data describes the query result, which contains column
 * definitions returned from the database.
 */
public interface QueryResultMetaData {

  /**
   * Get column count.
   *
   * @return column count
   */
  int getColumnCount();

  /**
   * Get schema name.
   *
   * @param columnIndex column index
   * @return schema name
   */
  String getSchemaName(int columnIndex);

  /**
   * Get table name.
   *
   * @param columnIndex column index
   * @return table name
   */
  String getTableName(int columnIndex) ;

  /**
   * Get table label.
   *
   * @param columnIndex column index
   * @return table label
   */
  String getTableLabel(int columnIndex) ;

  /**
   * Get column name.
   *
   * @param columnIndex column index
   * @return column name
   */
  String getColumnName(int columnIndex) ;

  /**
   * Get column label.
   *
   * @param columnIndex column index
   * @return column label
   */
  String getColumnLabel(int columnIndex) ;

  /**
   * Get column type.
   *
   * @param columnIndex column index
   * @return column type
   */
  MySQLColumnType getColumnType(int columnIndex) ;

  /**
   * Get column length.
   *
   * @param columnIndex column index
   * @return column length
   */
  int getColumnLength(int columnIndex) ;

  /**
   * Get decimals.
   *
   * @param columnIndex column index
   * @return decimals
   */
  int getDecimals(int columnIndex) ;

  /**
   * Get column flags.
   */
  int getColumnFlags(int columnIndex);
}
