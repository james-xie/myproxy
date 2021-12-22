package com.gllue.myproxy.common.concurrent.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SizedBlockingQueueTest {
  @Test
  public void testOfferAndPoll() {
    final int capacity = 10;
    var queue = prepareBlockingQueue(capacity);
    for (int i = 0; i < capacity; i++) {
      assertTrue(queue.offer(i));
    }
    assertFalse(queue.offer(0));
    assertFalse(queue.offer(0));

    assertEquals(Integer.valueOf(0), queue.poll());
    assertEquals(Integer.valueOf(1), queue.poll());
    assertTrue(queue.offer(0));
    assertTrue(queue.offer(0));
    assertEquals(capacity, queue.size());
  }

  @Test
  public void testIterator() {
    final int capacity = 10;
    var queue = prepareBlockingQueue(capacity);
    for (int i = 0; i < capacity; i++) {
      assertTrue(queue.offer(i));
    }

    var iterator = queue.iterator();
    for (int i = 0; i < capacity; i++) {
      assertTrue(iterator.hasNext());
      assertEquals(Integer.valueOf(i), iterator.next());
    }
  }

  @Test
  public void testForcePut() throws Exception {
    final int capacity = 10;
    var queue = prepareBlockingQueue(capacity);
    for (int i = 0; i < capacity; i++) {
      assertTrue(queue.offer(i));
    }

    var latch = new CountDownLatch(1);
    var t =
        new Thread(
            () -> {
              try {
                queue.forcePut(100);
                latch.countDown();
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            });
    t.start();
    queue.poll();
    latch.await();

    assertEquals(capacity, queue.size());
  }

  @Test
  public void testConcurrentOperation() throws Exception {
    final int capacity = 10000;
    var queue = prepareBlockingQueue(capacity);
    var threadPool = Executors.newFixedThreadPool(10);

    var latch1 = new CountDownLatch(50);
    for (int i = 0; i < 50; i++) {
      threadPool.submit(
          () -> {
            var random = new Random();
            for (int j = 0; j < 100; j++) {
              queue.offer(random.nextInt());
            }
            latch1.countDown();
          });
    }
    latch1.await();

    int queueSize = 50 * 100;
    assertEquals(queueSize, queue.size());

    var offerCount = new AtomicInteger();
    var pollCount = new AtomicInteger();
    var latch2 = new CountDownLatch(50);
    for (int i = 0; i < 50; i++) {
      threadPool.submit(
          () -> {
            var random = new Random();
            for (int j = 0; j < 100; j++) {
              if (random.nextBoolean()) {
                queue.offer(random.nextInt());
                offerCount.incrementAndGet();
              } else {
                queue.poll();
                pollCount.incrementAndGet();
              }
            }
            latch2.countDown();
          });
    }
    latch2.await();

    threadPool.shutdown();

    assertEquals(queueSize + offerCount.get() - pollCount.get(), queue.size());
  }

  SizedBlockingQueue<Integer> prepareBlockingQueue(final int capacity) {
    return new SizedBlockingQueue<>(new LinkedTransferQueue<>(), capacity);
  }
}
