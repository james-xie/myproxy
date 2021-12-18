package com.gllue.myproxy.common.concurrent.executor;

import com.gllue.myproxy.common.concurrent.DaemonThreadFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** A builder for fixed executors. */
public final class FixedExecutorBuilder extends ExecutorBuilder {

  private final int threads;
  private final int queueSize;

  /**
   * Construct a fixed executor builder.
   *
   * @param name the name of the executor
   * @param threads the fixed number of threads
   * @param queueSize the size of the backing queue, -1 for unbounded
   */
  public FixedExecutorBuilder(final String name, final int threads, final int queueSize) {
    super(name);
    assert queueSize > 0: "Queue size of fixed thread pool must be greater than 0";

    this.threads = threads;
    this.queueSize = queueSize;
  }

  private BlockingQueue<Runnable> blockingQueue() {
    var blockingQueue = new LinkedTransferQueue<Runnable>();
    return new SizedBlockingQueue<>(blockingQueue, queueSize);
  }

  private RejectedExecutionHandler rejectedExecutionHandler() {
    return new AbortPolicy();
  }

  private ThreadFactory threadFactory() {
    return new DaemonThreadFactory(String.format("%s-fixed", name()));
  }

  @Override
  public ExecutorService build() {
    return new ThreadPoolExecutor(
        threads,
        threads,
        0,
        TimeUnit.SECONDS,
        blockingQueue(),
        threadFactory(),
        rejectedExecutionHandler());
  }

}
