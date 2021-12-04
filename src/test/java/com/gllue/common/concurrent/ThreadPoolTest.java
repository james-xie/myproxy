package com.gllue.common.concurrent;

import static com.gllue.config.Configurations.Type.GENERIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.gllue.common.concurrent.ThreadPool.Name;
import com.gllue.config.Configurations;
import com.gllue.config.Configurations.Type;
import com.gllue.config.GenericConfigPropertyKey;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ThreadPoolTest {

  @Mock Configurations configurations;

  @Test
  public void testExecutor() throws InterruptedException {
    Mockito.when(configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.PROCESSORS))
        .thenReturn(4);

    var threadPool = prepareThreadPool();
    var countDown = new CountDownLatch(1);
    threadPool.executor(Name.GENERIC).submit(countDown::countDown);
    countDown.await();
    threadPool.shutdown();
  }

  @Test
  public void testGetStats() throws InterruptedException {
    Mockito.when(configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.PROCESSORS))
        .thenReturn(4);

    var threadPool = prepareThreadPool();
    var countDown = new CountDownLatch(1000);
    for (int i = 0; i < 1000; i++) {
      threadPool.executor(Name.GENERIC).submit(countDown::countDown);
    }
    countDown.await();

    var stats = threadPool.stats();
    boolean hasGenericExecutorStats = false;
    for (var stat : stats) {
      if (stat.getName().equals(Name.GENERIC.getName())) {
        hasGenericExecutorStats = true;
        assertTrue(stat.getThreads() >= 1);
        assertTrue(stat.getLargestThreads() >= 1);
        assertEquals(64, stat.getMaxPoolSize());
        assertEquals(0, stat.getRejectedTasks());
        assertEquals(1000, stat.getCompletedTasks());
      }
    }

    assertTrue(hasGenericExecutorStats);

    threadPool.shutdown();
  }

  @Test
  public void testShutdown() {
    Mockito.when(configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.PROCESSORS))
        .thenReturn(4);

    var threadPool = prepareThreadPool();
    threadPool.shutdown();
    assertTrue(threadPool.isShutdown());
    assertTrue(threadPool.executor(Name.GENERIC).isShutdown());
  }

  @Test
  public void testShutdownNow() {
    Mockito.when(configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.PROCESSORS))
        .thenReturn(4);

    var threadPool = prepareThreadPool();
    threadPool.shutdownNow();
    assertTrue(threadPool.isShutdown());
    assertTrue(threadPool.executor(Name.GENERIC).isShutdown());
  }

  ThreadPool prepareThreadPool() {
    Mockito.when(
            configurations.getValue(
                Type.GENERIC, GenericConfigPropertyKey.THREAD_POOL_FIXED_EXECUTOR_QUEUE_SIZE))
        .thenReturn(1000);
    return new ThreadPool(configurations);
  }
}
