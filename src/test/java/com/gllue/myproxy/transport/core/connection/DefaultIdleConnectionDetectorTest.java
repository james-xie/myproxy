package com.gllue.myproxy.transport.core.connection;

import static com.gllue.myproxy.constant.TimeConstants.MILLS_PER_SECOND;
import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultIdleConnectionDetectorTest {
  Map<Long, Connection> connectionMap = new ConcurrentHashMap<>();

  Connection newMockConnection(long lastAccessTime) {
    var connection = Mockito.mock(Connection.class);
    Mockito.when(connection.lastAccessTime()).thenReturn(lastAccessTime);
    return connection;
  }

  void batchRegisterConnections(IdleConnectionDetector detector, int endTime) {
    batchRegisterConnections(detector, 0, endTime);
  }

  void batchRegisterConnections(IdleConnectionDetector detector, int startTime, int endTime) {
    var workerCount = 5;
    var lastAccessTime = new AtomicLong(startTime);
    ExecutorService executor = Executors.newCachedThreadPool();
    var latch = new CountDownLatch(workerCount);
    for (int i = 0; i < workerCount; i++) {
      executor.execute(
          () -> {
            while (true) {
              var accessTime = lastAccessTime.addAndGet(MILLS_PER_SECOND);
              if (accessTime > endTime) {
                break;
              }
              var connection = newMockConnection(accessTime);
              connectionMap.put(accessTime, connection);
              detector.register(connection);
            }
            latch.countDown();
          });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    executor.shutdown();
  }

  void batchRemoveConnections(IdleConnectionDetector detector, int endTime) {
    batchRemoveConnections(detector, 0, endTime);
  }

  void batchRemoveConnections(IdleConnectionDetector detector, int startTime, int endTime) {
    var workerCount = 5;
    var lastAccessTime = new AtomicLong(startTime);
    ExecutorService executor = Executors.newCachedThreadPool();
    var latch = new CountDownLatch(workerCount);
    for (int i = 0; i < workerCount; i++) {
      executor.execute(
          () -> {
            while (true) {
              var accessTime = lastAccessTime.addAndGet(MILLS_PER_SECOND);
              if (accessTime > endTime) {
                break;
              }

              var connection = connectionMap.get(accessTime);
              if (connection != null) {
                detector.remove(connection);
                connectionMap.remove(accessTime);
              }
            }
            latch.countDown();
          });
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    executor.shutdown();
  }

  @Test
  public void testDetectorNotLazy() {
    var detector = new DefaultIdleConnectionDetector(10, false, false);
    batchRegisterConnections(detector, 1000 * MILLS_PER_SECOND);

    var idleConnections = detector.detectIdleConnections(20 * MILLS_PER_SECOND);
    assertEquals(10, idleConnections.size());

    batchRemoveConnections(detector, 500 * MILLS_PER_SECOND);
    idleConnections = detector.detectIdleConnections(1000 * MILLS_PER_SECOND);
    assertEquals(490, idleConnections.size());

    idleConnections = detector.detectIdleConnections(1000 * MILLS_PER_SECOND);
    assertEquals(0, idleConnections.size());
  }

  @Test
  public void testDetectorWithLazyRegister() {
    var detector = new DefaultIdleConnectionDetector(20, true, false);
    batchRegisterConnections(detector, 1000 * MILLS_PER_SECOND);

    var idleConnections = detector.detectIdleConnections(20 * MILLS_PER_SECOND);
    assertEquals(0, idleConnections.size());

    batchRemoveConnections(detector, 500 * MILLS_PER_SECOND);
    idleConnections = detector.detectIdleConnections(1000 * MILLS_PER_SECOND);
    assertEquals(480, idleConnections.size());

    idleConnections = detector.detectIdleConnections(1000 * MILLS_PER_SECOND);
    assertEquals(0, idleConnections.size());
  }

  @Test
  public void testDetectorWithLazyRemove() {
    var detector = new DefaultIdleConnectionDetector(20, false, true);
    batchRegisterConnections(detector, 1000 * MILLS_PER_SECOND);
    batchRemoveConnections(detector, 500 * MILLS_PER_SECOND);

    var idleConnections = detector.detectIdleConnections(20 * MILLS_PER_SECOND);
    assertEquals(0, idleConnections.size());

    idleConnections = detector.detectIdleConnections(1000 * MILLS_PER_SECOND);
    assertEquals(480, idleConnections.size());

    idleConnections = detector.detectIdleConnections(1000 * MILLS_PER_SECOND);
    assertEquals(0, idleConnections.size());
  }

  @Test
  public void testDetectorWithLazyRegisterAndLazyRemove() {
    var detector = new DefaultIdleConnectionDetector(100, true, true);
    batchRegisterConnections(detector, 1000 * MILLS_PER_SECOND);
    batchRemoveConnections(detector, 500 * MILLS_PER_SECOND);
    batchRegisterConnections(detector, 1000 * MILLS_PER_SECOND, 2000 * MILLS_PER_SECOND);
    batchRemoveConnections(detector, 500 * MILLS_PER_SECOND, 1000 * MILLS_PER_SECOND);

    var idleConnections = detector.detectIdleConnections(1000 * MILLS_PER_SECOND);
    assertEquals(0, idleConnections.size());

    idleConnections = detector.detectIdleConnections(1500 * MILLS_PER_SECOND);
    assertEquals(400, idleConnections.size());

    idleConnections = detector.detectIdleConnections(1500 * MILLS_PER_SECOND);
    assertEquals(0, idleConnections.size());
  }
}
