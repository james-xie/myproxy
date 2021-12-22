package com.gllue.myproxy.repository.zookeeper;

import com.gllue.myproxy.bootstrap.ServerContext;
import com.gllue.myproxy.common.Initializer;
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
    repository = new ZookeeperPersistRepository(zkProps);
    repository.init();
    context.setPersistRepository(repository);
  }

  @Override
  public void close() throws Exception {
    repository.close();
  }
}
