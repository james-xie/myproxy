package com.gllue.myproxy.common.concurrent.executor;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FixedExecutorBuilderTest {
  @Test
  public void testConcurrentExecution() throws InterruptedException, BrokenBarrierException {
    var builder = new FixedExecutorBuilder("fixed", 5, 10);
    var executorService = (ThreadPoolExecutor)builder.build();

    assertEquals(5, executorService.getCorePoolSize());
    assertEquals(5, executorService.getMaximumPoolSize());
    assertEquals(0, executorService.getActiveCount());
    assertEquals(10, executorService.getQueue().remainingCapacity());

    var latch = new CountDownLatch(5);
    var barrier = new CyclicBarrier(6);
    for (int i=0; i<10; i++) {
      executorService.submit(() -> {
        try {
          latch.countDown();
          barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          e.printStackTrace();
        }
      });
    }

    latch.await();

    assertEquals(5, executorService.getCorePoolSize());
    assertEquals(5, executorService.getMaximumPoolSize());
    assertEquals(5, executorService.getActiveCount());
    assertEquals(5, executorService.getQueue().remainingCapacity());

    barrier.await();
    executorService.shutdown();
  }

  @Test(expected = ExecutorRejectedExecutionException.class)
  public void testRejectionHandler() throws InterruptedException, BrokenBarrierException {
    var builder = new FixedExecutorBuilder("fixed", 5, 1);
    var executorService = (ThreadPoolExecutor)builder.build();

    var barrier = new CyclicBarrier(6);
    for (int i=0; i<10; i++) {
      executorService.submit(() -> {
        try {
          barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          e.printStackTrace();
        }
      });
    }

    barrier.await();
    executorService.shutdown();
  }


}
