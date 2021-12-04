package com.gllue.metadata;

import com.gllue.common.io.stream.StreamInput;

public interface MetaDataBuilder<MetaData> {
  void readStream(StreamInput input);

  void copyFrom(MetaData metadata, CopyOptions options);

  MetaData build();

  enum CopyOptions {
    DEFAULT,
    COPY_CHILDREN,
  }
}
