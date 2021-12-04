package com.gllue.command.result.query;

import com.gllue.transport.constant.MySQLColumnType;
import com.gllue.transport.protocol.packet.query.ColumnDefinition41Packet;
import java.util.List;

public class QueryResultMetaDataImpl implements QueryResultMetaData {
  private final ColumnDefinition41Packet[] columnDefArray;

  public QueryResultMetaDataImpl(final List<ColumnDefinition41Packet> columnDefList) {
    this.columnDefArray = columnDefList.toArray(new ColumnDefinition41Packet[0]);
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
