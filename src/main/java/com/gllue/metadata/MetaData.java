package com.gllue.metadata;

import com.gllue.common.io.stream.StreamInput;
import com.gllue.common.io.stream.StreamOutput;
import javax.annotation.Nullable;

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
