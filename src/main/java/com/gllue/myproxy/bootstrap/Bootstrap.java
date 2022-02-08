package com.gllue.myproxy.bootstrap;

import com.gllue.myproxy.cluster.ClusterStateInitializer;
import com.gllue.myproxy.common.Initializer;
import com.gllue.myproxy.common.concurrent.ThreadPoolInitializer;
import com.gllue.myproxy.common.generator.IdGeneratorInitializer;
import com.gllue.myproxy.repository.zookeeper.ZookeeperInitializer;
import com.gllue.myproxy.transport.backend.BackendServer;
import com.gllue.myproxy.transport.core.service.TransportServiceInitializer;
import com.gllue.myproxy.transport.frontend.FrontendServer;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.spi.LoggerContext;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

@Slf4j
public final class Bootstrap {
  private final FrontendServer frontendServer = FrontendServer.getInstance();
  private final BackendServer backendServer = BackendServer.getInstance();
  private final AtomicBoolean CLOSED = new AtomicBoolean(false);

  private final Initializer[] initializers =
      new Initializer[] {
        new ThreadPoolInitializer(),
        new ZookeeperInitializer(),
        new TransportServiceInitializer(),
        new ClusterStateInitializer(),
        new IdGeneratorInitializer(),
        backendServer,
        frontendServer,
      };

  public ServerContext initialize() throws Exception {
    var contextBuilder = new ServerContext.Builder();
    var context = contextBuilder.build();
    for (var initializer : initializers) {
      log.info("Initialize {}...", initializer.name());
      initializer.initialize(context);
    }
    log.info("Server has been initialized.");
    return context;
  }

  public void start() throws Exception {
    setShutdownGracefully();
    frontendServer.start();
  }

  public void close() {
    if (CLOSED.compareAndSet(false, true)) {
      try {
        for (int i = initializers.length - 1; i >= 0; i--) {
          try {
            log.info("Closing {}...", initializers[i].name());
            initializers[i].close();
          } catch (Exception e) {
            log.error("An exception occurred during closing {}...", initializers[i].name(), e);
          }
        }
      } catch (Exception e2) {
        log.error("An error is occurred when closing the bootstrap.", e2);
      }
    }
  }

  private void setShutdownGracefully() {
    Runtime.getRuntime().addShutdownHook(new Thread(this::close));
  }

  public static void main(String[] args) {
    var bootstrap = new Bootstrap();
    try {
      bootstrap.initialize();
      bootstrap.start();
    } catch (Throwable e1) {
      log.error("Closing bootstrap...", e1);
      bootstrap.close();
    }
  }
}
