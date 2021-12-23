package com.gllue.myproxy.command.result.query;

import com.gllue.myproxy.transport.constant.MySQLColumnType;
import lombok.RequiredArgsConstructor;

public class DefaultQueryResultMetaData implements QueryResultMetaData {
  private final Column[] columns;

  @RequiredArgsConstructor
  public static class Column {
    public final String schemaName;
    public final String tableName;
    public final String tableLabel;
    public final String columnName;
    public final String columnLabel;
    public final MySQLColumnType columnType;
    public final int columnLength;

    public Column(String columnName, MySQLColumnType columnType) {
      this("", "", "", columnName, "", columnType, 255);
    }
  }

  public DefaultQueryResultMetaData(Column[] columns) {
    this.columns = columns;
  }

  @Override
  public int getColumnCount() {
    return columns.length;
  }

  @Override
  public String getSchemaName(int columnIndex) {
    return columns[columnIndex].schemaName;
  }

  @Override
  public String getTableName(int columnIndex) {
    return columns[columnIndex].tableName;
  }

  @Override
  public String getTableLabel(int columnIndex) {
    return columns[columnIndex].tableLabel;
  }

  @Override
  public String getColumnName(int columnIndex) {
    return columns[columnIndex].columnName;
  }

  @Override
  public String getColumnLabel(int columnIndex) {
    return columns[columnIndex].columnLabel;
  }

  @Override
  public MySQLColumnType getColumnType(int columnIndex) {
    return columns[columnIndex].columnType;
  }

  @Override
  public int getColumnLength(int columnIndex) {
    return columns[columnIndex].columnLength;
  }

  @Override
  public int getDecimals(int columnIndex) {
    return 0;
  }
}
