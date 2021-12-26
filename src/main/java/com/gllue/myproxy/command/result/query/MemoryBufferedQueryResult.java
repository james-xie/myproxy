package com.gllue.myproxy.command.result.query;

import com.google.common.base.Preconditions;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MemoryBufferedQueryResult implements BufferedQueryResult {
  public static final int INITIAL_BUFFER_CAPACITY = 16;
  // default max capacity in bytes: 100 MB
  public static final int DEFAULT_MAX_CAPACITY_IN_BYTES = 100 * 1024 * 1024;
  public static final int DEFAULT_LOW_WATER_MARK_IN_BYTES = DEFAULT_MAX_CAPACITY_IN_BYTES / 3;
  public static final int DEFAULT_HIGH_WATER_MARK_IN_BYTES = DEFAULT_LOW_WATER_MARK_IN_BYTES * 2;
  public static final int THRESHOLD_OF_DISCARD_BUFFERED_QUERY_RESULT = 10 * 1024 * 1024;
  public static final int MAX_CAPACITY = 1 << 30;

  private final QueryResultMetaData metaData;
  private final int lowWaterMarkInBytes;
  private final int highWaterMarkInBytes;
  private final int maxCapacityInBytes;
  private final int discardThresholdInBytes;
  private int capacityInBytes;
  private int capacity;
  private int maxIndex;
  private int readerIndex = -1;
  private int writerIndex = -1;
  private boolean done;
  private volatile boolean hasWaitingThreads = false;
  private byte[][][] bufferedRows;
  @Setter private volatile Runnable writabilityChangedListener;

  public MemoryBufferedQueryResult(
      final QueryResultMetaData metaData,
      final int maxCapacityInBytes,
      final int lowWaterMarkInBytes,
      final int highWaterMarkInBytes,
      final int discardThresholdInBytes) {
    Preconditions.checkArgument(
        maxCapacityInBytes >= highWaterMarkInBytes,
        "maxCapacityInBytes must be greater than or equals to highWaterMarkInBytes");
    Preconditions.checkArgument(
        highWaterMarkInBytes >= lowWaterMarkInBytes,
        "highWaterMarkInBytes must be greater than or equals to lowWaterMarkInBytes");
    Preconditions.checkArgument(
        lowWaterMarkInBytes > 0, "lowWaterMarkInBytes must be greater than 0");

    this.capacityInBytes = 0;
    this.maxCapacityInBytes = maxCapacityInBytes;
    this.capacity = INITIAL_BUFFER_CAPACITY;
    this.maxIndex = this.capacity - 1;
    this.metaData = metaData;
    this.lowWaterMarkInBytes = lowWaterMarkInBytes;
    this.highWaterMarkInBytes = highWaterMarkInBytes;
    this.discardThresholdInBytes = discardThresholdInBytes;
    this.bufferedRows = new byte[capacity][][];
  }

  public MemoryBufferedQueryResult(final QueryResultMetaData metaData) {
    this(
        metaData,
        DEFAULT_MAX_CAPACITY_IN_BYTES,
        DEFAULT_LOW_WATER_MARK_IN_BYTES,
        DEFAULT_HIGH_WATER_MARK_IN_BYTES,
        THRESHOLD_OF_DISCARD_BUFFERED_QUERY_RESULT);
  }

  public MemoryBufferedQueryResult(
      final QueryResultMetaData metaData, final int maxCapacityInBytes) {
    this(
        metaData,
        maxCapacityInBytes,
        maxCapacityInBytes / 3,
        maxCapacityInBytes / 3 * 2,
        THRESHOLD_OF_DISCARD_BUFFERED_QUERY_RESULT);
  }

  public synchronized boolean addRow(final byte[][] row) {
    ensureWriterIndex();
    capacityInBytes += rowSize(row);
    ensureCapacityInBytes();

    this.bufferedRows[++writerIndex] = row;

    if (hasWaitingThreads) {
      wakeUpReadingThreads();
    }
    return capacityInBytes > highWaterMarkInBytes;
  }

  public synchronized void setDone() {
    done = true;
    if (hasWaitingThreads) {
      wakeUpReadingThreads();
    }
  }

  private int rowSize(byte[][] row) {
    int sizeInBytes = 0;
    for (var item : row) {
      sizeInBytes += item.length;
    }
    return sizeInBytes;
  }

  private void ensureWriterIndex() {
    if (writerIndex >= maxIndex) {
      if (readerIndex == -1 || readerIndex < (capacity >>> 1)) {
        ensureCapacity();
      } else {
        discardReadRows0();
      }
    }
  }

  private void ensureCapacityInBytes() {
    if (capacityInBytes > maxCapacityInBytes) {
      throw new BufferOutOfMemoryException(capacityInBytes);
    }
  }

  private void ensureCapacity() {
    int nextCapacity = capacity << 1;
    if (nextCapacity > MAX_CAPACITY) {
      throw new IndexOutOfBoundsException(
          String.format("writerIndex[%d] exceeds maxCapacity[%d]", writerIndex, capacity));
    }

    int length = readableRows();
    int offset = readerIndex + 1;
    byte[][][] oldBuffer = bufferedRows;
    bufferedRows = new byte[nextCapacity][][];
    System.arraycopy(oldBuffer, offset, bufferedRows, 0, length);
    capacity = nextCapacity;
    maxIndex = capacity - 1;
    readerIndex = -1;
    writerIndex -= offset;

    capacityInBytes = 0;
    for (int i = 0; i < length; i++) {
      capacityInBytes += rowSize(bufferedRows[i]);
    }
  }

  int readableRows() {
    return writerIndex - readerIndex;
  }

  private void resetIndex() {
    readerIndex = -1;
    writerIndex = -1;
    capacityInBytes = 0;
  }

  private synchronized void wakeUpReadingThreads() {
    notifyAll();
  }

  private void waitUntilReadable() {
    var timeBeforeWaiting = System.currentTimeMillis();
    log.info(
        "Waiting for the query result to be readable. [thread:{}]",
        Thread.currentThread().getName());

    hasWaitingThreads = true;

    synchronized (this) {
      while (!done && readerIndex >= writerIndex) {
        try {
          wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    hasWaitingThreads = false;

    var currentTime = System.currentTimeMillis();
    log.info(
        "Query result is readable again. [thread:{}, waitTime:{}ms]",
        Thread.currentThread().getName(),
        currentTime - timeBeforeWaiting);
  }

  @Override
  public synchronized boolean next() {
    if (readerIndex >= writerIndex) {
      resetIndex();
      if (done) {
        return false;
      }
      waitUntilReadable();
      if (done && readerIndex >= writerIndex) {
        return false;
      }
    } else {
      discardSomeReadRows();
    }

    readerIndex++;

    if (capacityInBytes < lowWaterMarkInBytes && writabilityChangedListener != null) {
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

  private synchronized void discardReadRows0() {
    int length = readableRows();
    System.arraycopy(bufferedRows, readerIndex + 1, bufferedRows, 0, length);
    readerIndex = -1;
    writerIndex = length - 1;
    capacityInBytes = 0;
    for (int i = 0; i < length; i++) {
      capacityInBytes += rowSize(bufferedRows[i]);
    }
  }

  @Override
  public synchronized void discardReadRows() {
    if (readerIndex > -1) {
      if (readerIndex == writerIndex) {
        resetIndex();
      } else {
        discardReadRows0();
      }
    }
  }

  @Override
  public void discardSomeReadRows() {
    if (readerIndex > -1) {
      if (readerIndex == writerIndex) {
        resetIndex();
      } else if (readerIndex >= (capacity >>> 1) && capacityInBytes >= discardThresholdInBytes) {
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
