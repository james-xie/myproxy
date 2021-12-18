package com.gllue.myproxy.bootstrap;

import com.gllue.myproxy.transport.backend.BackendServer;
import com.gllue.myproxy.transport.frontend.FrontendServer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class Bootstrap {
  FrontendServer frontendServer = FrontendServer.getInstance();
  BackendServer backendServer = BackendServer.getInstance();

  public ServerContext initialize() throws Exception {
    var contextBuilder = new ServerContext.Builder();

    var context = contextBuilder.build();
    backendServer.initialize(context);
    frontendServer.initialize(context);
    log.info("Server has been initialized.");
    return context;
  }

  public void start() throws Exception {
    frontendServer.start();
  }

  public void close() throws Exception {
    frontendServer.close();
    backendServer.close();
  }

  public static void main(String[] args) {
    var bootstrap = new Bootstrap();
    try {
      bootstrap.initialize();
      bootstrap.start();
    } catch (Throwable e1) {
      try {
        log.error("Closing bootstrap...", e1);
        bootstrap.close();
      } catch (Exception e2) {
        log.error("An error is occurred when closing the bootstrap.", e2);
      }
    }
  }
}
