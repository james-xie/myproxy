package com.gllue.myproxy.command.result.query;

import com.gllue.myproxy.transport.constant.MySQLColumnType;
import com.gllue.myproxy.transport.protocol.packet.query.ColumnDefinition41Packet;

public class ColumnDefQueryResultMetaData implements QueryResultMetaData {
  private final ColumnDefinition41Packet[] columnDefArray;

  public ColumnDefQueryResultMetaData(final ColumnDefinition41Packet[] columnDefList) {
    this.columnDefArray = columnDefList;
  }

  @Override
  public int getColumnCount() {
    return columnDefArray.length;
  }

  @Override
  public String getSchemaName(int columnIndex) {
    return columnDefArray[columnIndex].getSchema();
  }

  @Override
  public String getTableName(int columnIndex) {
    return columnDefArray[columnIndex].getOrgTable();
  }

  @Override
  public String getTableLabel(int columnIndex) {
    return columnDefArray[columnIndex].getTable();
  }

  @Override
  public String getColumnName(int columnIndex) {
    return columnDefArray[columnIndex].getOrgName();
  }

  @Override
  public String getColumnLabel(int columnIndex) {
    return columnDefArray[columnIndex].getName();
  }

  @Override
  public MySQLColumnType getColumnType(int columnIndex) {
    return MySQLColumnType.valueOf(columnDefArray[columnIndex].getColumnType());
  }

  @Override
  public int getColumnLength(int columnIndex) {
    return columnDefArray[columnIndex].getColumnLength();
  }

  @Override
  public int getDecimals(int columnIndex) {
    return columnDefArray[columnIndex].getDecimals();
  }
}
