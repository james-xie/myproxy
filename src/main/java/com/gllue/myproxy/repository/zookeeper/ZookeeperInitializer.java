package com.gllue.myproxy.repository.zookeeper;

import com.gllue.myproxy.bootstrap.ServerContext;
import com.gllue.myproxy.common.Initializer;
import com.gllue.myproxy.common.concurrent.ThreadPool.Name;
import com.gllue.myproxy.repository.ClusterPersistRepository;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ZookeeperInitializer implements Initializer {
  private ClusterPersistRepository repository;

  @Override
  public String name() {
    return "zookeeper";
  }

  @Override
  public void initialize(ServerContext context) {
    var zkProps = new ZookeeperConfigProperties(context.getProperties());
    var executorService = context.getThreadPool().executor(Name.GENERIC);
    repository = new ZookeeperPersistRepository(zkProps, executorService);
    repository.init();
    context.setPersistRepository(repository);
  }

  @Override
  public void close() throws Exception {
    if (repository != null) repository.close();
  }
}
