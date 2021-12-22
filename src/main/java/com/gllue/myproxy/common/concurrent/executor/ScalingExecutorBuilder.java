package com.gllue.myproxy.common.concurrent.executor;

import com.gllue.myproxy.common.concurrent.DaemonThreadFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** A builder for scaling executors. */
public final class ScalingExecutorBuilder extends ExecutorBuilder {

  private final int corePoolSize;
  private final int maxPoolSize;
  private final int queueSize;
  private final int keepAliveTime;
  private final TimeUnit unit;

  /**
   * Construct a scaling executor builder.
   *
   * @param name the name of the executor
   * @param corePoolSize the core pool size of executor service
   * @param maxPoolSize the max pool size of executor service
   * @param keepAliveTime the keep alive time of executor service
   * @param unit the keep alive time unit of executor service
   */
  public ScalingExecutorBuilder(
      final String name,
      final int corePoolSize,
      final int maxPoolSize,
      final int queueSize,
      final int keepAliveTime,
      final TimeUnit unit) {
    super(name);
    this.corePoolSize = corePoolSize;
    this.maxPoolSize = maxPoolSize;
    this.queueSize = queueSize;
    this.keepAliveTime = keepAliveTime;
    this.unit = unit;
  }

  private BlockingQueue<Runnable> blockingQueue() {
    if (queueSize == 0) {
      return new SynchronousQueue<>();
    }

    var blockingQueue = new LinkedTransferQueue<Runnable>();
    return new SizedBlockingQueue<>(blockingQueue, queueSize);
  }

  private RejectedExecutionHandler rejectedExecutionHandler() {
    return new WaitingQueueAvailablePolicy();
  }

  private ThreadFactory threadFactory() {
    return new DaemonThreadFactory(String.format("%s-scaling", name()));
  }

  @Override
  public ExecutorService build() {
    return new ThreadPoolExecutor(
        corePoolSize,
        maxPoolSize,
        keepAliveTime,
        unit,
        blockingQueue(),
        threadFactory(),
        rejectedExecutionHandler());
  }

}
