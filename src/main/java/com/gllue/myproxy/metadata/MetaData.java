package com.gllue.myproxy.metadata;

import com.gllue.myproxy.common.io.stream.StreamInput;

public interface MetaData {
  int DEFAULT_VERSION = 0;

  String getIdentity();

  int getVersion();

  interface Builder<M> {
    void readStream(StreamInput input);

    void copyFrom(M metadata);

    M build();
  }
}
