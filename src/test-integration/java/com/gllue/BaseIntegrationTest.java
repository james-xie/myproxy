package com.gllue;

import com.gllue.bootstrap.Bootstrap;
import com.gllue.bootstrap.ServerContext;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;

@Slf4j
public abstract class BaseIntegrationTest {
  private static boolean started = false;
  private static ServerContext serverContext;

  public ServerContext getServerContext() {
    return serverContext;
  }

  @Before
  public void before() {
    if (started) {
      return;
    }

    synchronized (BaseIntegrationTest.class) {
      if (!started) {
        var latch = new CountDownLatch(1);
        runOnNewThread(() -> {
          var bootstrap = new Bootstrap();
          try {
            serverContext = bootstrap.initialize();
            latch.countDown();
            bootstrap.start();
          } catch (Throwable e1) {
            try {
              log.error("Closing bootstrap...", e1);
              bootstrap.close();
            } catch (Exception e2) {
              log.error("An error is occurred when closing the bootstrap.", e2);
            }
          }

        });
        try {
          latch.await();
          // Waiting for server started.
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        started = true;
      }
    }
  }

  public void runOnNewThread(Runnable runnable) {
    var thread = new Thread(runnable);
    thread.setDaemon(true);
    thread.start();
  }


}
