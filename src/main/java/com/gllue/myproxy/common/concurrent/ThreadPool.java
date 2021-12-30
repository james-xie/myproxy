package com.gllue.myproxy.common.concurrent;

import static com.gllue.myproxy.config.Configurations.Type.GENERIC;

import com.gllue.myproxy.common.concurrent.ThreadPoolStats.ExecutorServiceStats;
import com.gllue.myproxy.common.concurrent.executor.AccountableRejectedExecutionHandler;
import com.gllue.myproxy.common.concurrent.executor.ExecutorBuilder;
import com.gllue.myproxy.common.concurrent.executor.FixedExecutorBuilder;
import com.gllue.myproxy.common.concurrent.executor.ScalingExecutorBuilder;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.config.GenericConfigPropertyKey;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

public class ThreadPool {
  @Getter
  @RequiredArgsConstructor
  enum Type {
    DIRECT("direct"),
    FIXED("fixed"),
    SCALING("scaling");

    private final String type;
  }

  @Getter
  @RequiredArgsConstructor
  public enum Name {
    GENERIC("generic", Type.SCALING),
    COMMAND("command", Type.FIXED);

    private final String name;
    private final Type type;
  }

  private boolean isShutdown;

  private final Map<Name, ExecutorService> executors;
  private final Configurations configurations;

  public ThreadPool(final Configurations configurations) {
    int processors = configurations.getValue(GENERIC, GenericConfigPropertyKey.PROCESSORS);
    this.configurations = configurations;
    this.executors = buildExecutors(processors);
  }

  private Map<Name, ExecutorService> buildExecutors(final int processors) {
    var executors = new HashMap<Name, ExecutorService>();
    addExecutor(executors, Name.GENERIC, genericExecutorBuilder(processors));
    addExecutor(executors, Name.COMMAND, fixedExecutorBuilder(processors));

    return Collections.unmodifiableMap(executors);
  }

  private ExecutorBuilder genericExecutorBuilder(final int processors) {
    int maxPoolSize = boundedBy(4 * processors, 64, 256);
    return new ScalingExecutorBuilder(
        Name.GENERIC.name, processors, maxPoolSize, 0, 30, TimeUnit.SECONDS);
  }

  private ExecutorBuilder fixedExecutorBuilder(final int processors) {
    int maxPoolSize = 2 * processors;
    int maxQueueSize =
        configurations.getValue(
            GENERIC, GenericConfigPropertyKey.THREAD_POOL_FIXED_EXECUTOR_QUEUE_SIZE);
    return new FixedExecutorBuilder(Name.GENERIC.name, maxPoolSize, maxQueueSize);
  }

  private void addExecutor(
      Map<Name, ExecutorService> executors, Name name, ExecutorBuilder builder) {
    switch (name.type) {
      case FIXED:
        Preconditions.checkArgument(builder instanceof FixedExecutorBuilder);
        break;
      case SCALING:
        Preconditions.checkArgument(builder instanceof ScalingExecutorBuilder);
        break;
    }

    var old = executors.put(name, builder.build());
    assert old == null;
  }

  /**
   * Get the executor service with the given name.
   *
   * @param name the name of the executor service to obtain
   * @throws IllegalArgumentException if no executor service with the specified name exists
   */
  public ExecutorService executor(Name name) {
    final ExecutorService executorService = executors.get(name);
    if (executorService == null) {
      throw new IllegalArgumentException("No such executor service [" + name + "]");
    }
    return executorService;
  }

  public ScheduledExecutorService scheduler() {
    return null;
  }

  /**
   * Generate statistics info for the thread pool.
   *
   * @return statistics info
   */
  public ThreadPoolStats stats() {
    List<ExecutorServiceStats> stats = new ArrayList<>();
    for (var entry : executors.entrySet()) {
      String name = entry.getKey().name;
      int corePoolSize = -1;
      int maxPoolSize = -1;
      int threads = -1;
      int queueSize = -1;
      int activeThreads = -1;
      long rejected = -1;
      int largestThreads = -1;
      long completedTasks = -1;
      var executorService = entry.getValue();
      if (executorService instanceof ThreadPoolExecutor) {
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
        corePoolSize = threadPoolExecutor.getCorePoolSize();
        maxPoolSize = threadPoolExecutor.getMaximumPoolSize();
        threads = threadPoolExecutor.getPoolSize();
        queueSize = threadPoolExecutor.getQueue().size();
        activeThreads = threadPoolExecutor.getActiveCount();
        largestThreads = threadPoolExecutor.getLargestPoolSize();
        completedTasks = threadPoolExecutor.getCompletedTaskCount();
        var rejectedHandler = threadPoolExecutor.getRejectedExecutionHandler();
        if (rejectedHandler instanceof AccountableRejectedExecutionHandler) {
          rejected = ((AccountableRejectedExecutionHandler) rejectedHandler).rejectedExecutions();
        }
      }
      stats.add(
          new ExecutorServiceStats(
              name,
              corePoolSize,
              maxPoolSize,
              threads,
              activeThreads,
              largestThreads,
              queueSize,
              rejected,
              completedTasks));
    }
    return new ThreadPoolStats(stats);
  }

  /**
   * Constrains a value between minimum and maximum values (inclusive).
   *
   * @param value the value to constrain
   * @param min the minimum acceptable value
   * @param max the maximum acceptable value
   * @return min if value is less than min, max if value is greater than value, otherwise value
   */
  static int boundedBy(int value, int min, int max) {
    return Math.min(max, Math.max(min, value));
  }

  public static final ExecutorService DIRECT_EXECUTOR_SERVICE =
      new AbstractExecutorService() {

        @Override
        public void shutdown() {
          throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public List<Runnable> shutdownNow() {
          throw new UnsupportedOperationException();
        }

        @Override
        public boolean isShutdown() {
          return false;
        }

        @Override
        public boolean isTerminated() {
          return false;
        }

        @Override
        public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit)
            throws InterruptedException {
          throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable command) {
          command.run();
        }
      };

  public void shutdown() {
    if (isShutdown) {
      return;
    }

    synchronized (executors) {
      if (!isShutdown) {
        for (var executor : executors.values()) {
          executor.shutdown();
        }
        isShutdown = true;
      }
    }
  }

  public List<Runnable> shutdownNow() {
    if (isShutdown) {
      return List.of();
    }

    var tasks = new ArrayList<Runnable>();
    synchronized (executors) {
      if (!isShutdown) {
        for (var executor : executors.values()) {
          tasks.addAll(executor.shutdownNow());
        }
        isShutdown = true;
      }
    }
    return tasks;
  }

  /**
   * Returns {@code true} if this executor has been shut down.
   *
   * @return {@code true} if this executor has been shut down
   */
  boolean isShutdown() {
    return isShutdown;
  }
}
