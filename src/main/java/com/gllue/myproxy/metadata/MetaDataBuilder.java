package com.gllue.myproxy.metadata;

public interface MetaDataBuilder<MetaData> {

  void copyFrom(MetaData metadata, CopyOptions options);

  MetaData build();

  enum CopyOptions {
    DEFAULT,
    COPY_CHILDREN,
  }
}
