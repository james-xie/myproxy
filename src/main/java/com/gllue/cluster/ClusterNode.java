package com.gllue.cluster;

import lombok.Getter;

@Getter
public class ClusterNode {
  private final int nodeId;
  private final String nodeName;

  public ClusterNode(final int nodeId) {
    this.nodeId = nodeId;
    this.nodeName = generateName(nodeId);
  }

  private String generateName(int nodeId) {
    return "node-" + nodeId;
  }
}
