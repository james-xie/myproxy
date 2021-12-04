package com.gllue.cluster;

import com.gllue.metadata.model.MultiDatabasesMetaData;
import lombok.Getter;

public final class ClusterState {
  private final ClusterNode current;
  @Getter private final ClusterNode[] nodes;
  @Getter private final MultiDatabasesMetaData metaData;

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
