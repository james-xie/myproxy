package com.gllue.myproxy.command.result.query;

public class MemoryCachedQueryResult implements QueryResult {
  public static final int INITIAL_CAPACITY = 8;
  public static final int MAX_CAPACITY = 1 << 30;
  // default max capacity in bytes: 100 MB
  public static final int DEFAULT_MAX_CAPACITY_IN_BYTES = 100 * 1024 * 1024;

  private final QueryResultMetaData queryResultMetaData;
  private final int maxCapacityInBytes;
  private int capacity;
  private int maxIndex;
  private int readerIndex = -1;
  private int writerIndex = -1;
  private int capacityInBytes = 0;
  private byte[][][] rows;

  public MemoryCachedQueryResult(final QueryResultMetaData queryResultMetaData) {
    this(queryResultMetaData, DEFAULT_MAX_CAPACITY_IN_BYTES);
  }

  public MemoryCachedQueryResult(
      final QueryResultMetaData queryResultMetaData, final int maxCapacityInBytes) {
    this.queryResultMetaData = queryResultMetaData;
    this.maxCapacityInBytes = maxCapacityInBytes;
    this.capacity = INITIAL_CAPACITY;
    this.maxIndex = capacity - 1;
    this.rows = new byte[capacity][][];
  }

  @Override
  public boolean next() {
    if (readerIndex >= writerIndex) {
      return false;
    }

    readerIndex++;
    return true;
  }

  public void addRow(final byte[][] row) {
    capacityInBytes += rowSize(row);
    ensureCapacityInBytes();

    ensureCapacity();
    rows[++writerIndex] = row;
  }

  private void ensureCapacityInBytes() {
    if (capacityInBytes > maxCapacityInBytes) {
      throw new QueryResultOutOfMemoryException(capacityInBytes);
    }
  }

  private void ensureCapacity() {
    if (writerIndex >= maxIndex) {
      int nextCapacity = capacity << 1;
      if (nextCapacity > MAX_CAPACITY) {
        throw new IndexOutOfBoundsException(
            String.format("writerIndex[%d] exceeds maxCapacity[%d]", writerIndex, capacity));
      }
      var oldRows = rows;
      rows = new byte[nextCapacity][][];
      System.arraycopy(oldRows, 0, rows, 0, oldRows.length);
      capacity = nextCapacity;
      maxIndex = capacity - 1;
    }
  }

  private int rowSize(byte[][] row) {
    int sizeInBytes = 0;
    for (var item : row) {
      sizeInBytes += item.length;
    }
    return sizeInBytes;
  }

  @Override
  public byte[] getValue(int columnIndex) {
    return rows[readerIndex][columnIndex];
  }

  @Override
  public String getStringValue(int columnIndex) {
    return new String(getValue(columnIndex));
  }

  @Override
  public QueryResultMetaData getMetaData() {
    return queryResultMetaData;
  }

  @Override
  public void close() {
    this.rows = null;
  }
}
