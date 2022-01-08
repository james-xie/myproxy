package com.gllue.myproxy.transport.core.connection;

import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.google.common.base.Preconditions;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AdaptableTrafficThrottlePipe implements TrafficThrottlePipe {
  private static final int INITIAL_FLUSH_THRESHOLD = 4;
  private static final int INITIAL_SLOW_START_THRESHOLD = 128;

  private final Connection in;
  private final Connection out;
  private final WritabilityChangedListener<Connection> writabilityChangedListener;

  private boolean prepared = false;
  private int flushThreshold;
  private int slowStartThreshold;
  private int pendingWritePackets = 0;

  public AdaptableTrafficThrottlePipe(final Connection in, final Connection out) {
    Preconditions.checkNotNull(in);
    Preconditions.checkNotNull(out);
    this.in = in;
    this.out = out;

    this.flushThreshold = INITIAL_FLUSH_THRESHOLD;
    this.slowStartThreshold = INITIAL_SLOW_START_THRESHOLD;
    this.writabilityChangedListener =
        new WritabilityChangedListener<>() {
          @Override
          public Executor executor() {
            return ThreadPool.DIRECT_EXECUTOR_SERVICE;
          }

          @Override
          public void onSuccess(Connection result) {
            if (log.isDebugEnabled()) {
              log.info("Connection-{} writability changed.", result.connectionId());
            }
            in.enableAutoRead();
          }
        };
  }

  @Override
  public void prepareToTransfer() {
    if (!prepared) {
      out.addWritabilityChangedListener(writabilityChangedListener);
      prepared = true;
    }
  }

  private void tryFlush() {
    if (!out.isWritable() && in.isAutoRead()) {
      in.disableAutoRead();
      out.flush();

      slowStartThreshold = Math.max(flushThreshold >>> 1, 1);
      flushThreshold = slowStartThreshold + 1;
      pendingWritePackets = 0;

      if (log.isDebugEnabled()) {
        log.debug(
            "Connection-{} is not writable, waiting for the writability changed.",
            out.connectionId());
      }
    } else if (++pendingWritePackets > flushThreshold) {
      out.flush();
      pendingWritePackets = 0;
      if (flushThreshold < slowStartThreshold) {
        flushThreshold = Math.min(slowStartThreshold, flushThreshold << 1);
      } else {
        flushThreshold++;
      }
      flushThreshold = Math.max(1, flushThreshold);
    }
  }

  @Override
  public boolean transfer(final MySQLPacket packet, final boolean forceFlush) {
    if (out.isClosed()) {
      return false;
    }

    out.write(packet);
    if (forceFlush) {
      out.flush();
    } else {
      tryFlush();
    }
    return true;
  }

  @Override
  public void close() throws Exception {
    out.removeWritabilityChangedListener(writabilityChangedListener);
  }
}
