package com.gllue.myproxy.command.result.query;

import lombok.Setter;

public class BufferedQueryResult implements QueryResult {
  public static final int INITIAL_BUFFER_CAPACITY = 8;
  public static final int MAX_BUFFER_CAPACITY = 1 << 20;
  public static final int DEFAULT_BUFFER_LOW_WATER_MARK = 1 << 14;
  public static final int DEFAULT_BUFFER_HIGH_WATER_MARK = DEFAULT_BUFFER_LOW_WATER_MARK << 1;
  public static final int MAX_CAPACITY_IN_BYTES = 1 << 28;

  private byte[][][] bufferedRows;
  private final QueryResultMetaData metaData;
  private final int bufferLowWaterMark;
  private final int bufferHighWaterMark;
  private int capacity;
  private int maxCapacity;
  private int readerIndex = 0;
  private int writerIndex = 0;
  @Setter private volatile Runnable writabilityChangedListener;

  public BufferedQueryResult(
      final QueryResultMetaData metaData,
      final int initCapacity,
      final int maxCapacity,
      final int bufferLowWaterMark,
      final int bufferHighWaterMark) {
    this.capacity = initCapacity;
    this.maxCapacity = maxCapacity;
    this.metaData = metaData;
    this.bufferLowWaterMark = bufferLowWaterMark;
    this.bufferHighWaterMark = bufferHighWaterMark;
    this.bufferedRows = new byte[capacity][][];
  }

  public BufferedQueryResult(final QueryResultMetaData metaData) {
    this(metaData, INITIAL_BUFFER_CAPACITY);
  }

  public BufferedQueryResult(final QueryResultMetaData metaData, final int initCapacity) {
    this(
        metaData,
        initCapacity,
        MAX_BUFFER_CAPACITY,
        DEFAULT_BUFFER_LOW_WATER_MARK,
        DEFAULT_BUFFER_HIGH_WATER_MARK);
  }

  public boolean addRow(final byte[][] row) {
    ensureWriterIndex();
    this.bufferedRows[writerIndex++] = row;
    return readableRows() >= bufferHighWaterMark;
  }

  private void ensureWriterIndex() {
    if (writerIndex >= capacity) {
      if (readerIndex == 0) {
        ensureCapacity();
      } else {
        discardReadRows0();
      }
    }
  }

  private void ensureCapacity() {
    assert readerIndex == 0 && writerIndex >= capacity;

    int nextCapacity = capacity << 1;
    if (nextCapacity > maxCapacity) {
      throw new IndexOutOfBoundsException(
          String.format("writerIndex[%d] exceeds maxCapacity[%d]", writerIndex, capacity));
    }
    byte[][][] oldBuffer = bufferedRows;
    bufferedRows = new byte[nextCapacity][][];
    System.arraycopy(oldBuffer, 0, bufferedRows, 0, capacity);
    capacity = nextCapacity;
  }

  private int readableRows() {
    return writerIndex - readerIndex;
  }

  @Override
  public boolean next() {
    if (readerIndex >= writerIndex) {
      readerIndex = 0;
      writerIndex = 0;
      return false;
    }
    readerIndex++;

    if (readableRows() < bufferLowWaterMark && writabilityChangedListener != null) {
      writabilityChangedListener.run();
      writabilityChangedListener = null;
    }

    return true;
  }

  @Override
  public byte[] getValue(int columnIndex) {
    return bufferedRows[readerIndex][columnIndex];
  }

  @Override
  public String getStringValue(int columnIndex) {
    return new String(getValue(columnIndex));
  }

  private void discardReadRows0() {
    int length = readableRows();
    System.arraycopy(bufferedRows, readerIndex, bufferedRows, 0, length);
    readerIndex = 0;
    writerIndex = length;
  }

  @Override
  public void discardReadRows() {
    if (readerIndex > 0) {
      if (readerIndex == writerIndex) {
        writerIndex = readerIndex = 0;
      } else {
        discardReadRows0();
      }
    }
  }

  @Override
  public void discardSomeReadRows() {
    if (readerIndex > 0) {
      if (readerIndex == writerIndex) {
        writerIndex = readerIndex = 0;
      } else if (readerIndex >= readableRows() >>> 1) {
        discardReadRows0();
      }
    }
  }

  @Override
  public QueryResultMetaData getMetaData() {
    return metaData;
  }

  @Override
  public void close() {
    bufferedRows = null;
  }
}
