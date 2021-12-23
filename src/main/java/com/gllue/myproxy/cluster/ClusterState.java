package com.gllue.myproxy.cluster;

import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import lombok.Getter;

@Getter
public class ClusterState {

  private final ClusterNode current;
  private final ClusterNode[] nodes;
  private final MultiDatabasesMetaData metaData;

  public ClusterState(
      final ClusterNode current, final ClusterNode[] nodes, final MultiDatabasesMetaData metaData) {
    this.current = current;
    this.nodes = nodes;
    this.metaData = metaData;
  }

  public ClusterNode currentNode() {
    return current;
  }
}
