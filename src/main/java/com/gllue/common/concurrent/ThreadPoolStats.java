package com.gllue.common.concurrent;

import com.gllue.common.concurrent.ThreadPoolStats.ExecutorServiceStats;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Getter;

/** The statistics info for the thread pool. */
public class ThreadPoolStats implements Iterable<ExecutorServiceStats> {

  @Getter
  public static class ExecutorServiceStats implements Comparable<ExecutorServiceStats> {
    private final String name;
    private final int corePoolSize;
    private final int maxPoolSize;
    private final int threads;
    private final int activeThreads;
    private final int largestThreads;
    private final int queueSize;
    private final long rejectedTasks;
    private final long completedTasks;

    /**
     * Construct a ExecutorServiceStats object.
     *
     * @param name the executor service name
     * @param corePoolSize the core number of threads
     * @param maxPoolSize the maximum allowed number of threads
     * @param threads the current number of threads in the pool
     * @param activeThreads the approximate number of threads that are actively executing tasks
     * @param largestThreads the largest number of threads that have ever simultaneously been in the
     *     pool
     * @param queueSize the executor service blocking queue capacity
     * @param rejectedTasks The number of rejected executions
     * @param completedTasks the approximate total number of tasks that have completed execution
     */
    public ExecutorServiceStats(
        final String name,
        final int corePoolSize,
        final int maxPoolSize,
        final int threads,
        final int activeThreads,
        final int largestThreads,
        final int queueSize,
        final long rejectedTasks,
        final long completedTasks) {
      Preconditions.checkNotNull(name);
      this.name = name;
      this.corePoolSize = corePoolSize;
      this.maxPoolSize = maxPoolSize;
      this.threads = threads;
      this.activeThreads = activeThreads;
      this.largestThreads = largestThreads;
      this.queueSize = queueSize;
      this.rejectedTasks = rejectedTasks;
      this.completedTasks = completedTasks;
    }

    @Override
    public int compareTo(ExecutorServiceStats other) {
      int res = this.name.compareTo(other.name);
      if (res == 0) {
        res = Integer.compare(threads, other.threads);
      }
      return res;
    }
  }

  private final List<ExecutorServiceStats> stats;

  public ThreadPoolStats(List<ExecutorServiceStats> stats) {
    Collections.sort(stats);
    this.stats = stats;
  }

  @Nonnull
  @Override
  public Iterator<ExecutorServiceStats> iterator() {
    return this.stats.iterator();
  }
}
