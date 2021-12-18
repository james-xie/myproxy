package com.gllue.myproxy.metadata;

import com.gllue.myproxy.common.io.stream.StreamInput;
import com.gllue.myproxy.common.io.stream.StreamOutput;

public interface MetaData {
  int DEFAULT_VERSION = 0;

  String getIdentity();

  int getVersion();

  /** Write this object's fields to a {@linkplain StreamOutput}. */
  void writeTo(StreamOutput output);


  interface Builder<M> {
    void readStream(StreamInput input);

    void copyFrom(M metadata);

    M build();
  }
}
