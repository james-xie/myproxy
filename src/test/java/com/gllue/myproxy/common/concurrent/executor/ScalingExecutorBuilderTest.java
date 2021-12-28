package com.gllue.myproxy.common.concurrent.executor;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ScalingExecutorBuilderTest {
  @Test
  public void testConcurrentExecution() throws InterruptedException, BrokenBarrierException {
    var builder = new ScalingExecutorBuilder("scaling", 5, 10, 0, 30, TimeUnit.SECONDS);
    var executorService = (ThreadPoolExecutor) builder.build();

    assertEquals(5, executorService.getCorePoolSize());
    assertEquals(10, executorService.getMaximumPoolSize());
    assertEquals(0, executorService.getActiveCount());
    assertEquals(0, executorService.getQueue().remainingCapacity());
    assertEquals(30, executorService.getKeepAliveTime(TimeUnit.SECONDS));

    var latch = new CountDownLatch(10);
    var barrier = new CyclicBarrier(11);
    for (int i = 0; i < 10; i++) {
      executorService.submit(
          () -> {
            try {
              latch.countDown();
              barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
              e.printStackTrace();
            }
          });
    }

    latch.await();

    assertEquals(10, executorService.getActiveCount());
    assertEquals(0, executorService.getQueue().remainingCapacity());

    barrier.await();
    executorService.shutdown();
  }

  @Test
  public void testRejectionHandler() throws InterruptedException, BrokenBarrierException {
    var builder = new ScalingExecutorBuilder("scaling", 5, 10, 0, 30, TimeUnit.SECONDS);
    var executorService = (ThreadPoolExecutor) builder.build();

    var latch = new CountDownLatch(11);
    var counter = new CountDownLatch(15);

    // The thread will be blocked when submitting tasks to the executor service, so we must
    // start a new thread to finish this job.
    var thread =
        new Thread(
            () -> {
              for (int i = 0; i < 15; i++) {
                executorService.submit(
                    () -> {
                      try {
                        latch.countDown();
                        latch.await();
                        counter.countDown();
                      } catch (InterruptedException e) {
                        e.printStackTrace();
                      }
                    });
              }
            });
    thread.start();

    // All tasks are hangs on the latch.await(), so the counter has no effects.
    assertEquals(15, counter.getCount());
    latch.countDown();
    latch.await();

    counter.await();
    assertEquals(0, counter.getCount());

    executorService.shutdown();
  }
}
