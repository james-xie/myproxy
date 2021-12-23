package com.gllue.myproxy.common.generator;

import com.gllue.myproxy.bootstrap.ServerContext;
import com.gllue.myproxy.common.Initializer;

public class IdGeneratorInitializer implements Initializer {
  private SnowflakeIdGenerator idGenerator;

  @Override
  public String name() {
    return "id generator";
  }

  @Override
  public void initialize(ServerContext context) {
    var clusterState = context.getClusterState();
    idGenerator = new SnowflakeIdGenerator(clusterState.getCurrent().getNodeId());
    context.setIdGenerator(idGenerator);
  }

  @Override
  public void close() throws Exception {
    // todo: save the last time stamp.
  }
}
