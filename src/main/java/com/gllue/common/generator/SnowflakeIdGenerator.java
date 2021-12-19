package com.gllue.common.generator;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SnowflakeIdGenerator implements IdGenerator {
  private static final long START_TIMESTAMP = 1639830490000L;
  private static final long SEQUENCE_BITS = 14;
  private static final long NODE_ID_BITS = 8;
  private static final long NODE_ID_SHIFT = SEQUENCE_BITS;
  private static final long TIMESTAMP_SHIFT = NODE_ID_BITS + SEQUENCE_BITS;
  private static final long MAX_NODE_ID = ~(-1L << NODE_ID_BITS);
  private static final long SEQUENCE_ID_MASK = ~(-1L << SEQUENCE_BITS);

  @Getter private final long nodeId;
  private long sequenceId = 0L;
  private long lastTimestamp = -1L;

  public SnowflakeIdGenerator(final long nodeId) {
    if (nodeId > MAX_NODE_ID || nodeId < 0) {
      throw new IllegalArgumentException(
          String.format("Node id can't be greater than %d or less than 0", MAX_NODE_ID));
    }

    this.nodeId = nodeId;
  }

  public void checkForClockMovingBackwards(final long lastTimestamp, final int maxTimeDiff)
      throws InterruptedException {
    var lastTime = lastTimestamp + maxTimeDiff;
    var currentTime = currentTimeMillis();
    while (currentTime <= lastTime) {
      log.warn("Clock is moving backwards. Sleeping until {}.", lastTime - currentTime);
      Thread.sleep(lastTime - currentTime);
      currentTime = currentTimeMillis();
    }
  }

  @Override
  public synchronized long nextId() {
    var timestamp = currentTimeMillis();
    if (timestamp < lastTimestamp) {
      log.error("Clock is moving backwards. Rejecting requests until {}.", lastTimestamp);
      throw new InvalidSystemClock(
          "Clock moved backwards. Refusing to generate id for %d milliseconds",
          lastTimestamp - timestamp);
    }
    if (timestamp == lastTimestamp) {
      sequenceId = (sequenceId + 1) & SEQUENCE_ID_MASK;
      if (sequenceId == 0) {
        timestamp = nextMillis(lastTimestamp);
      }
    } else {
      sequenceId = 0;
    }

    lastTimestamp = timestamp;
    return ((timestamp - START_TIMESTAMP) << TIMESTAMP_SHIFT)
        | (nodeId << NODE_ID_SHIFT)
        | sequenceId;
  }

  private long nextMillis(long lastTimestamp) {
    var timestamp = currentTimeMillis();
    while (timestamp <= lastTimestamp) {
      timestamp = currentTimeMillis();
    }
    return timestamp;
  }

  private long currentTimeMillis() {
    return System.currentTimeMillis();
  }
}
