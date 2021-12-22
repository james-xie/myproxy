package com.gllue.repository.zookeeper;

import com.gllue.bootstrap.ServerContext;
import com.gllue.common.Initializer;
import com.gllue.repository.ClusterPersistRepository;
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
