package com.gllue.myproxy.command.result.query;

public class ReusableRow implements Row {
  private final QueryResultMetaData metaData;
  private byte[][] rowData;

  public ReusableRow(final QueryResultMetaData metaData) {
    this.metaData = metaData;
  }

  public void setRowData(byte[][] rowData) {
    assert metaData.getColumnCount() == rowData.length;
    this.rowData = rowData;
  }

  @Override
  public QueryResultMetaData getMetaData() {
    return metaData;
  }

  @Override
  public byte[][] getRowData() {
    return rowData;
  }

  @Override
  public byte[] getValue(int columnIndex) {
    return rowData[columnIndex];
  }

  @Override
  public <T> T getTypedValue(int columnIndex) {
    // todo: implement it.
    return null;
  }
}
